"""
Mô phỏng công thức tổng quát của DBSP:   S^Δ = 𝒟 ∘ S ∘ ℐ

- Stream   : biểu diễn bằng list (một đoạn đầu hữu hạn của hàm ℕ → A).
- Z-set    : dict {row: weight}, weight > 0 là insert, < 0 là delete.
- ℐ, 𝒟, z⁻¹, ↑ (lift), ∘ (compose) : các toán tử trên stream.
"""
from __future__ import annotations
from typing import Callable, Hashable, TypeVar

A = TypeVar("A")
B = TypeVar("B")
Stream = list  # Stream[A] ≈ 𝒮_A, chỉ giữ một prefix hữu hạn


# ───────────────────────── Z-set (nhóm Abel) ─────────────────────────
class ZSet:
    def __init__(self, data: dict[Hashable, int] | None = None):
        self.data = {k: w for k, w in (data or {}).items() if w != 0}

    @staticmethod
    def of(*rows, weight: int = 1) -> "ZSet":
        z: dict = {}
        for r in rows:
            z[r] = z.get(r, 0) + weight
        return ZSet(z)

    def __add__(self, other: "ZSet") -> "ZSet":
        out = dict(self.data)
        for k, w in other.data.items():
            out[k] = out.get(k, 0) + w
        return ZSet(out)

    def __neg__(self) -> "ZSet":
        return ZSet({k: -w for k, w in self.data.items()})

    def __sub__(self, other: "ZSet") -> "ZSet":
        return self + (-other)

    def __eq__(self, other) -> bool:
        return isinstance(other, ZSet) and self.data == other.data

    def __repr__(self) -> str:
        if not self.data:
            return "∅"
        return "{" + ", ".join(f"{k}:{w:+d}" for k, w in sorted(self.data.items())) + "}"


class Pair:
    """Database có 2 bảng (SCH = ZSet × ZSet), cộng/trừ theo từng thành phần."""
    def __init__(self, a: ZSet, b: ZSet):
        self.a, self.b = a, b
    def __add__(self, o): return Pair(self.a + o.a, self.b + o.b)
    def __neg__(self):    return Pair(-self.a, -self.b)
    def __sub__(self, o): return self + (-o)
    def __eq__(self, o):  return isinstance(o, Pair) and self.a == o.a and self.b == o.b
    def __repr__(self):   return f"({self.a}, {self.b})"


def zero_like(x):
    return x - x


# ───────────────────────── Toán tử trên stream ─────────────────────────
def delay(s: Stream) -> Stream:
    """z⁻¹ : (z⁻¹ s)[t] = s[t-1], với s[-1] = 0."""
    return [zero_like(s[0])] + s[:-1] if s else []


def I(s: Stream) -> Stream:
    """ℐ : tích luỹ.  ℐ(s)[t] = s[0] + ... + s[t]."""
    out, acc = [], None
    for x in s:
        acc = x if acc is None else acc + x
        out.append(acc)
    return out


def D(s: Stream) -> Stream:
    """𝒟 : vi phân.  𝒟(s)[t] = s[t] - s[t-1]."""
    return [x - p for x, p in zip(s, delay(s))]


def lift(f: Callable[[A], B]) -> Callable[[Stream], Stream]:
    """↑f : áp f lên từng phần tử của stream (pointwise)."""
    return lambda s: [f(x) for x in s]


def compose(*fs: Callable) -> Callable:
    """compose(f, g, h)(x) = f(g(h(x)))   — đọc từ phải sang trái như ∘."""
    def h(x):
        for f in reversed(fs):
            x = f(x)
        return x
    return h


def incremental(S: Callable[[Stream], Stream]) -> Callable[[Stream], Stream]:
    """ĐỊNH NGHĨA tổng quát:  S^Δ = 𝒟 ∘ S ∘ ℐ"""
    return compose(D, S, I)


# ───────────────────────── Một số query mẫu ─────────────────────────
def filter_big(z: ZSet) -> ZSet:            # σ_{amount > 100}  (tuyến tính)
    return ZSet({r: w for r, w in z.data.items() if r[2] > 100})


def count(z: ZSet) -> ZSet:                 # SELECT COUNT(*)  (không tuyến tính)
    return ZSet.of((sum(z.data.values()),))


def join(users: ZSet, orders: ZSet) -> ZSet:  # users ⋈ orders ON uid (song tuyến tính)
    out: dict = {}
    for (uid, name), wu in users.data.items():
        for (oid, ouid, amt), wo in orders.data.items():
            if uid == ouid:
                k = (name, oid, amt)
                out[k] = out.get(k, 0) + wu * wo
    return ZSet(out)


def stable_rows(s: Stream) -> Stream:
    """Streaming query KHÔNG pointwise: hàng có mặt ở cả t và t-1.
    S(s)[t] = s[t] ∩ s[t-1]  → phụ thuộc quá khứ qua z⁻¹."""
    def inter(x: ZSet, y: ZSet) -> ZSet:
        return ZSet({k: min(w, y.data[k]) for k, w in x.data.items() if k in y.data})
    return [inter(x, y) for x, y in zip(s, delay(s))]


def join_incremental_fast(T: Stream) -> Stream:
    """Cài đặt hiệu quả nhờ quy tắc song tuyến tính:
    Δ(a⋈b) = Δa⋈Δb + Δa⋈b_cũ + a_cũ⋈Δb   — chỉ giữ state a_cũ, b_cũ."""
    out = []
    a_old, b_old = ZSet(), ZSet()
    for t in T:
        out.append(join(t.a, t.b) + join(t.a, b_old) + join(a_old, t.b))
        a_old, b_old = a_old + t.a, b_old + t.b
    return out


# ───────────────────────── Demo ─────────────────────────
if __name__ == "__main__":
    # T: stream các transaction (thay đổi) trên bảng orders(oid, uid, amount)
    T = [
        ZSet.of((1, 1, 50), (2, 1, 200)),          # t=0: insert 2 đơn
        ZSet.of((3, 2, 150)),                      # t=1: insert 1 đơn
        ZSet.of((2, 1, 200), weight=-1),           # t=2: delete đơn 2
        ZSet.of((4, 2, 300)),                      # t=3: insert 1 đơn
    ]

    print("T        =", T)
    print("DB = ℐ(T) =", I(T))
    print("𝒟(ℐ(T))  =", D(I(T)), " (= T, vì 𝒟 và ℐ nghịch đảo)\n")

    # 1) Query tuyến tính: S^Δ = ↑Q  →  chạy thẳng Q trên thay đổi
    S = lift(filter_big)
    print("[filter] S^Δ(T)       =", incremental(S)(T))
    print("[filter] ↑Q(T)        =", S(T))
    print("         bằng nhau?    ", incremental(S)(T) == S(T), "\n")

    # 2) Query không tuyến tính: COUNT → ΔV gồm xoá giá trị cũ, thêm giá trị mới
    print("[count]  V  = ↑Q(ℐ(T)) =", lift(count)(I(T)))
    print("[count]  ΔV = S^Δ(T)   =", incremental(lift(count))(T), "\n")

    # 3) Join song tuyến tính: so sánh định nghĩa với cài đặt hiệu quả
    TJ = [
        Pair(ZSet.of((1, "An")),            ZSet()),
        Pair(ZSet(),                        ZSet.of((10, 1, 99))),
        Pair(ZSet.of((2, "Binh")),          ZSet.of((11, 2, 42))),
        Pair(ZSet.of((1, "An"), weight=-1), ZSet()),
    ]
    by_def = incremental(lift(lambda db: join(db.a, db.b)))(TJ)
    fast = join_incremental_fast(TJ)
    print("[join]   theo định nghĩa 𝒟∘↑Q∘ℐ =", by_def)
    print("[join]   theo quy tắc bilinear   =", fast)
    print("         bằng nhau?              ", by_def == fast, "\n")

    # 4) Streaming query tổng quát S (không phải ↑Q)
    print("[stable] S^Δ(T) =", incremental(stable_rows)(T))