"""DBSP biến một câu query thành phiên bản chỉ chạy trên phần thay đổi như thế nào.

Query trong file này:   SELECT game, sum(amount) FROM clicks WHERE amount >= 50 GROUP BY game

- Z-set: dict {dòng: trọng số}. Trọng số +1 là dòng được thêm vào, -1 là dòng bị xoá.
- Stream: list các Z-set, mỗi epoch một Z-set.
- Kafka (không có CDC) chỉ append: mỗi message vào là một dòng với trọng số +1.
  Trọng số -1 chỉ xuất hiện ở đầu ra của aggregate, khi tổng của một game thay đổi.

Chạy: python3 notes/dbsp_rules.py
"""
from typing import Callable, Dict, List, Tuple, TypeVar

R = TypeVar("R")
A = TypeVar("A")
B = TypeVar("B")

Event = Tuple[str, int]   # một message Kafka: (game, amount)
Total = Tuple[str, int]   # một dòng kết quả của view: (game, tổng amount)
ZSet = Dict[R, int]       # dòng -> trọng số
Stream = List[ZSet[R]]    # mỗi epoch một Z-set


# ---------------------------------------------------------------------------
# Phép cộng, trừ Z-set
# ---------------------------------------------------------------------------

def plus(a: ZSet[R], b: ZSet[R]) -> ZSet[R]:
    """Cộng hai Z-set: trọng số của cùng một dòng được cộng lại, dòng nào về 0 thì bỏ."""
    result: ZSet[R] = dict(a)
    for row, weight in b.items():
        result[row] = result.get(row, 0) + weight
        if result[row] == 0:
            del result[row]
    return result


def minus(a: ZSet[R], b: ZSet[R]) -> ZSet[R]:
    """a - b: những gì đã thay đổi để đi từ b sang a."""
    negated = {row: -weight for row, weight in b.items()}
    return plus(a, negated)


# ---------------------------------------------------------------------------
# I và D: hai toán tử trên stream
# ---------------------------------------------------------------------------

def integrate(changes: Stream[R]) -> Stream[R]:
    """I: stream các thay đổi -> stream các bảng đầy đủ (mỗi epoch: cộng dồn mọi thứ tới giờ)."""
    tables: Stream[R] = []
    total: ZSet[R] = {}
    for change in changes:
        total = plus(total, change)
        tables.append(total)
    return tables


def differentiate(tables: Stream[R]) -> Stream[R]:
    """D: stream các bảng đầy đủ -> stream các thay đổi (mỗi epoch: bảng này trừ bảng trước)."""
    changes: Stream[R] = []
    previous: ZSet[R] = {}
    for table in tables:
        changes.append(minus(table, previous))
        previous = table
    return changes


# ---------------------------------------------------------------------------
# Hai toán tử của query, chạy trên một Z-set
# ---------------------------------------------------------------------------

def where_big(table: ZSet[Event]) -> ZSet[Event]:
    """WHERE amount >= 50.

    Tuyến tính: xét từng dòng riêng lẻ, nên chạy trên phần thay đổi vẫn ra kết quả đúng.
    """
    return {row: weight for row, weight in table.items() if row[1] >= 50}


def sum_per_game(table: ZSet[Event]) -> ZSet[Total]:
    """SELECT game, sum(amount) GROUP BY game: mỗi game một dòng (game, tổng).

    Không tuyến tính: tổng của một game phụ thuộc vào mọi dòng của game đó,
    không chỉ những dòng mới tới.
    """
    totals: Dict[str, int] = {}
    for (game, amount), weight in table.items():
        totals[game] = totals.get(game, 0) + amount * weight
    return {(game, total): 1 for game, total in totals.items() if total != 0}


def every_epoch(operator: Callable[[ZSet[A]], ZSet[B]], stream: Stream[A]) -> Stream[B]:
    """Chạy một toán tử trên Z-set của từng epoch trong stream."""
    return [operator(table) for table in stream]


# ---------------------------------------------------------------------------
# Aggregate tăng dần: thứ mà các luật biến sum_per_game thành
# ---------------------------------------------------------------------------

def sum_per_game_incremental(changes: Stream[Event], log: bool = False) -> Stream[Total]:
    """Các thay đổi của đầu ra sum_per_game, tính chỉ từ các thay đổi của đầu vào.

    Giữ tổng của từng game làm state (DuckWake giữ nó trong một bảng DuckLake).
    Mỗi epoch, với mỗi game có dữ liệu mới: xoá dòng (game, tổng cũ) bằng -1
    và thêm dòng (game, tổng mới) bằng +1.
    """
    totals: Dict[str, int] = {}
    output: Stream[Total] = []
    for epoch, change in enumerate(changes, 1):
        out: ZSet[Total] = {}
        for (game, amount), weight in change.items():
            old = totals.get(game, 0)
            new = old + amount * weight
            totals[game] = new
            if old != 0:
                out = plus(out, {(game, old): -1})
            if new != 0:
                out = plus(out, {(game, new): 1})
        if log:
            print(f"  epoch {epoch}: state sau epoch = {totals}")
        output.append(out)
    return output


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def fmt(z: ZSet[R]) -> str:
    """Một Z-set dạng +dòng / -dòng."""
    if not z:
        return "(trống)"
    return ", ".join(f"{'+' if w > 0 else '-'}{row}" for row, w in sorted(z.items(), key=str))


def show(title: str, stream: Stream[R]) -> None:
    """In một stream, mỗi epoch một dòng."""
    print(title)
    for epoch, z in enumerate(stream, 1):
        print(f"  epoch {epoch}: {fmt(z)}")


if __name__ == "__main__":
    # Kafka gửi tới, mỗi epoch một lô. Không có CDC nên mọi trọng số đều là +1.
    kafka: Stream[Event] = [
        {("g1", 60): 1, ("g2", 10): 1},
        {("g1", 70): 1, ("g2", 80): 1},
        {("g1", 90): 1},
    ]
    show("Bước 0. Kafka gửi tới (chỉ có +1):", kafka)

    print("\n=== Cách chậm, đúng theo định nghĩa: D ∘ Q ∘ I ===")
    whole = integrate(kafka)
    show("Bước 1. I: cộng dồn thành bảng đầy đủ:", whole)
    big = every_epoch(where_big, whole)
    show("Bước 2. Q, phần WHERE amount >= 50:", big)
    results = every_epoch(sum_per_game, big)
    show("Bước 3. Q, phần sum per game (kết quả đầy đủ của view):", results)
    slow = differentiate(results)
    show("Bước 4. D: kết quả epoch này trừ kết quả epoch trước:", slow)

    print("\n=== Cách nhanh, sau khi áp dụng các luật: AggΔ ∘ Filter ===")
    big_changes = every_epoch(where_big, kafka)
    show("Bước 1. Lọc ngay trên phần thay đổi (WHERE là tuyến tính):", big_changes)
    print("Bước 2. Aggregate tăng dần, chỉ đọc phần thay đổi và state:")
    fast = sum_per_game_incremental(big_changes, log=True)
    show("Bước 3. Đầu ra:", fast)

    assert fast == slow
    print("\nHai cách ra cùng một kết quả.")

    # Ba luật, kiểm tra trên cùng dữ liệu.
    # 1. D ∘ I = id: cộng dồn rồi lấy hiệu thì ra lại đúng các thay đổi ban đầu.
    assert differentiate(integrate(kafka)) == kafka
    # 2. Toán tử tuyến tính đổi chỗ được với I: lọc bảng đầy đủ = lọc phần thay đổi rồi cộng dồn.
    assert every_epoch(where_big, integrate(kafka)) == integrate(every_epoch(where_big, kafka))
    # 3. Toán tử không tuyến tính thành dạng tăng dần: Agg ∘ I = I ∘ AggΔ.
    assert every_epoch(sum_per_game, integrate(kafka)) == integrate(sum_per_game_incremental(kafka))
    print("Ba luật đều đúng.")
