//! Each view kind, epoch by epoch, against the batch query (see `harness`).
use super::harness::check_view;

const EPOCHS: [(i64, i64); 5] = [(0, 7), (7, 40), (40, 41), (41, 200), (200, 260)];

#[test]
fn stateless() {
    check_view("SELECT * FROM clicks WHERE amount > 5", &EPOCHS, false);
    check_view(
        "SELECT c.ts, g.studio, c.amount * 2 AS a2 FROM clicks c JOIN games g ON g.game_id = c.game_id WHERE c.amount > 3",
        &EPOCHS,
        false,
    );
}

#[test]
fn distinct() {
    let sql = "SELECT DISTINCT game_id, user_id FROM clicks WHERE amount > 2";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    // NULL keys, and rows that repeat within an epoch and across epochs.
    check_view("SELECT DISTINCT g.studio, c.amount FROM clicks c LEFT JOIN games g ON g.game_id = c.game_id", &EPOCHS, true);
}

#[test]
fn windowed() {
    let tumble = "SELECT window_start, game_id, count(*) AS n, count(amount) AS n_amount, sum(amount) AS revenue,
                         avg(amount) AS mean, min(amount) AS lo, max(ts) AS last_ts
                  FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    check_view(tumble, &EPOCHS, false);
    check_view(tumble, &EPOCHS, true);
    check_view(
        "SELECT c.window_end, g.studio, count(*) AS n, sum(c.amount) AS s
         FROM HOP(clicks, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND) c JOIN games g ON g.game_id = c.game_id
         WHERE c.amount > 0 GROUP BY c.window_end, g.studio",
        &EPOCHS,
        false,
    );
}

#[test]
fn global_group_by_all_with_having() {
    let sql = "SELECT game_id, count(*) AS n, count(amount) AS n_amount, sum(amount) AS total, avg(amount) AS mean,
                      min(amount) AS lo, max(amount) AS hi
               FROM clicks GROUP BY ALL HAVING avg(amount) > 5.5";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn global_alias_key_and_expressions_over_aggregates() {
    check_view(
        "SELECT upper(game_id) AS g, sum(amount) / count(*) AS per_click, count(*) + 1 AS n1
         FROM clicks WHERE user_id <> 'u3' GROUP BY g HAVING sum(amount) > 50",
        &EPOCHS,
        false,
    );
}

#[test]
fn global_with_lookup_join() {
    // With HAVING, groups leave and re-enter the table.
    let sql = "SELECT g.studio, count(*) AS n, max(c.amount) AS top FROM clicks c JOIN games g ON g.game_id = c.game_id
               GROUP BY 1 HAVING count(*) % 2 = 0";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    check_view("SELECT g.studio, count(*) AS n, max(c.amount) AS top FROM clicks c JOIN games g ON g.game_id = c.game_id GROUP BY 1", &EPOCHS, false);
}

#[test]
fn global_without_group_by() {
    // The single group is touched by every epoch.
    let sql = "SELECT count(*) AS n, sum(amount) AS s, avg(amount) AS a FROM clicks WHERE amount > 5";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}
