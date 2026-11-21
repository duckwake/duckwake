//! Each view kind, epoch by epoch, against the batch query (see `harness`).
use super::harness::{check_upsert, check_view};
use crate::plan::UpsertSpec;

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
fn union_all() {
    // Two sources, with different column names, a lookup join and a filter in one branch.
    let sql = "SELECT ts, game_id, amount AS value FROM clicks WHERE amount > 4
               UNION ALL SELECT p.ts, g.studio, p.price FROM purchases p JOIN games g ON g.game_id = p.game_id";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    // Three branches, one source read twice.
    check_view(
        "SELECT ts, user_id FROM clicks WHERE amount > 8 UNION ALL SELECT ts, user_id FROM purchases
         UNION ALL SELECT ts, game_id FROM clicks WHERE amount < 2",
        &EPOCHS,
        false,
    );
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

#[test]
fn stateless_over_retractions() {
    // A counted sink: each row with its multiplicity.
    check_view("SELECT * FROM changes WHERE amount > 5", &EPOCHS, false);
    check_view("SELECT c.ts, g.studio, c.amount FROM changes c JOIN games g ON g.game_id = c.game_id", &EPOCHS, true);
    // Few distinct rows, so most counts are above 1 and go up and down.
    check_view("SELECT game_id, user_id FROM changes", &EPOCHS, true);
}

#[test]
fn distinct_over_retractions() {
    // Rows leave the table when their last copy is deleted or updated away.
    let sql = "SELECT DISTINCT game_id, user_id FROM changes WHERE amount > 2";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn global_over_retractions() {
    let sql = "SELECT game_id, count(*) AS n, count(amount) AS n_amount, sum(amount) AS total, avg(amount) AS mean
               FROM changes GROUP BY ALL HAVING count(*) > 3";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    check_view("SELECT count(*) AS n, sum(amount) AS s FROM changes WHERE amount > 5", &EPOCHS, true);
}

#[test]
fn union_with_a_retracting_branch() {
    let sql = "SELECT ts, game_id, amount AS value FROM changes WHERE amount > 4 UNION ALL SELECT ts, game_id, price FROM purchases";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn windowed_over_retractions() {
    // Weighted partials; a window and group whose rows are all retracted has no row.
    let tumble = "SELECT window_start, game_id, count(*) AS n, count(amount) AS n_amount, sum(amount) AS revenue,
                         avg(amount) AS mean
                  FROM TUMBLE(changes, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    check_view(tumble, &EPOCHS, false);
    check_view(tumble, &EPOCHS, true);
    check_view(
        "SELECT c.window_end, g.studio, user_id, count(*) AS n, sum(c.amount) AS s
         FROM HOP(changes, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND) c JOIN games g ON g.game_id = c.game_id
         WHERE c.amount > 3 GROUP BY ALL",
        &EPOCHS,
        true,
    );
}

#[test]
fn global_min_max_over_retractions() {
    // A group's extreme is deleted or updated away: the next value still present takes its place, and NULL
    // once none is left. Two MINs and a MAX over different arguments share the value rows.
    let sql = "SELECT game_id, min(amount) AS lo, max(amount) AS hi, min(user_id) AS first_user, count(*) AS n
               FROM changes GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    check_view("SELECT max(amount) AS hi, min(ts) AS first_ts FROM changes WHERE amount > 3", &EPOCHS, true);
    check_view(
        "SELECT g.studio, user_id, max(c.amount) AS top FROM changes c JOIN games g ON g.game_id = c.game_id GROUP BY ALL HAVING count(*) > 2",
        &EPOCHS,
        true,
    );
}

#[test]
fn windowed_variance_booleans_and_having() {
    // Every variance and standard deviation, and bool_and/bool_or (MIN/MAX of a BOOLEAN), per window.
    let stats = "SELECT window_start, game_id, var_samp(amount) AS vs, var_pop(amount) AS vp, variance(amount) AS v,
                        stddev_samp(amount) AS ss, stddev_pop(amount) AS sp, stddev(amount) AS s,
                        bool_and(amount > 3) AS all_big, bool_or(amount > 10) AS any_huge
                 FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    check_view(stats, &EPOCHS, false);
    check_view(stats, &EPOCHS, true);
    // HAVING over an aggregate that is not selected, a selected key, and a select item by its alias.
    let having = "SELECT window_start, game_id, count(*) AS n, stddev_pop(amount) AS sd
                  FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL
                  HAVING sum(amount) > 40 AND game_id <> 'g2' AND n % 3 <> 0";
    check_view(having, &EPOCHS, false);
    check_view(having, &EPOCHS, true);
    check_view(
        "SELECT c.window_end, g.studio, count(*) AS n FROM HOP(clicks, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND) c
         JOIN games g ON g.game_id = c.game_id GROUP BY c.window_end, g.studio HAVING avg(c.amount) > 6",
        &EPOCHS,
        false,
    );
}

#[test]
fn windowed_variance_and_having_over_retractions() {
    let sql = "SELECT window_start, game_id, count(*) AS n, var_samp(amount) AS vs, stddev_pop(amount) AS sp
               FROM TUMBLE(changes, ts, INTERVAL 1 MINUTE) GROUP BY ALL HAVING count(*) > 4";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn global_variance_and_booleans() {
    let sql = "SELECT game_id, var_samp(amount) AS vs, var_pop(amount) AS vp, stddev_samp(amount) AS ss,
                      stddev_pop(amount) AS sp, bool_and(amount > 2) AS all_pos, bool_or(user_id = 'u4') AS has_u4
               FROM clicks GROUP BY ALL HAVING stddev(amount) > 1";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    check_view("SELECT variance(price) AS v, stddev(price) AS s FROM purchases", &EPOCHS, true);
}

#[test]
fn global_variance_and_booleans_over_retractions() {
    // Retracted values leave the sums; a group down to one value has no sample variance again; bool_and's
    // false value retracted makes it true again.
    let sql = "SELECT game_id, count(*) AS n, var_samp(amount) AS vs, stddev_pop(amount) AS sp, bool_and(amount > 4) AS all_big
               FROM changes GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn global_count_distinct() {
    // Several count(DISTINCT) over different arguments, next to MIN and MAX, which then share the value rows.
    let sql = "SELECT game_id, count(DISTINCT user_id) AS users, count(DISTINCT amount) AS amounts, count(*) AS n,
                      min(amount) AS lo, max(user_id) AS last_user
               FROM clicks GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    // No GROUP BY, NULLs not counted, and HAVING on a distinct count.
    check_view("SELECT count(DISTINCT game_id) AS games, count(DISTINCT amount) AS amounts FROM clicks", &EPOCHS, true);
    check_view(
        "SELECT g.studio, count(DISTINCT c.user_id) AS users FROM clicks c JOIN games g ON g.game_id = c.game_id
         GROUP BY 1 HAVING count(DISTINCT c.user_id) > 3",
        &EPOCHS,
        false,
    );
}

#[test]
fn global_count_distinct_over_retractions() {
    // A value stays counted while any of its rows is left; a group whose values are all retracted counts 0.
    let sql = "SELECT game_id, count(DISTINCT user_id) AS users, count(DISTINCT amount) AS amounts, max(amount) AS hi
               FROM changes GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    check_view("SELECT count(DISTINCT user_id) AS users FROM changes WHERE amount > 3", &EPOCHS, true);
}

#[test]
fn windowed_distinct_counts_and_quantiles() {
    // count(DISTINCT) and approx_count_distinct keep each window's distinct values, the quantiles every value;
    // NULLs are left out, a list of fractions gives a list, and a TIMESTAMP has quantiles too.
    let sql = "SELECT window_start, game_id, count(DISTINCT user_id) AS users, approx_count_distinct(user_id) AS approx_users,
                      median(amount) AS med, quantile_cont(amount, 0.9) AS p90, quantile_disc(amount, [0.25, 0.75]) AS quartiles,
                      quantile(ts, 0.5) AS mid_ts, count(*) AS n
               FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    // In HAVING, over hopping windows, and through a lookup join.
    check_view(
        "SELECT c.window_end, g.studio, count(DISTINCT c.user_id) AS users, median(c.amount) AS med
         FROM HOP(clicks, ts, INTERVAL 1 MINUTE, INTERVAL 20 SECOND) c JOIN games g ON g.game_id = c.game_id
         GROUP BY c.window_end, g.studio HAVING count(DISTINCT c.user_id) > 3 AND quantile_cont(c.amount, 0.25) > 2",
        &EPOCHS,
        true,
    );
}

#[test]
fn windowed_covariance_correlation_and_regression() {
    let all = "covar_pop(amount, second(ts)) AS cp, covar_samp(amount, second(ts)) AS cs, corr(amount, second(ts)) AS r,
               regr_count(amount, second(ts)) AS rn, regr_avgx(amount, second(ts)) AS ax, regr_avgy(amount, second(ts)) AS ay,
               regr_sxx(amount, second(ts)) AS sxx, regr_syy(amount, second(ts)) AS syy, regr_sxy(amount, second(ts)) AS sxy,
               regr_slope(amount, second(ts)) AS slope, regr_intercept(amount, second(ts)) AS icpt, regr_r2(amount, second(ts)) AS r2";
    let sql = format!("SELECT window_start, game_id, {all} FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL");
    check_view(&sql, &EPOCHS, false);
    check_view(&sql, &EPOCHS, true);
    // Grouped by amount, y is constant in every group: corr is NaN, regr_r2 1, and stddev exactly 0 (not a
    // rounding residue); x constant gives NaN slopes and NULL intercepts. Rows with a NULL on either side are
    // left out of every pair.
    let constant = "SELECT window_start, amount, stddev_pop(amount) AS sd, corr(amount, second(ts)) AS r, regr_r2(amount, second(ts)) AS r2,
                           regr_slope(second(ts), 7) AS flat, regr_intercept(second(ts), 7) AS no_icpt, covar_samp(amount, 1) AS c1
                    FROM TUMBLE(clicks, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    check_view(constant, &EPOCHS, false);
}

#[test]
fn windowed_covariance_over_retractions() {
    // The paired sums are invertible, so a window over a source that retracts keeps them like avg.
    let sql = "SELECT window_start, game_id, count(*) AS n, covar_samp(amount, second(ts)) AS cs, regr_slope(amount, second(ts)) AS slope
               FROM TUMBLE(changes, ts, INTERVAL 1 MINUTE) GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn global_quantiles_and_approx_count_distinct() {
    let sql = "SELECT game_id, median(amount) AS med, quantile_disc(amount, 0.9) AS p90, quantile_cont(amount, [0.1, 0.5]) AS qs,
                      approx_count_distinct(user_id) AS users, count(DISTINCT user_id) AS exact_users, min(amount) AS lo
               FROM clicks GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    check_view("SELECT median(price) AS m, approx_count_distinct(game_id) AS g FROM purchases HAVING median(price) > 3", &EPOCHS, true);
}

#[test]
fn global_quantiles_over_retractions() {
    // A value's count goes down as its rows are retracted; a group with no value left has NULL quantiles and
    // counts 0.
    let sql = "SELECT game_id, count(*) AS n, median(amount) AS med, quantile_disc(amount, 0.75) AS p75, approx_count_distinct(amount) AS amounts
               FROM changes GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

#[test]
fn global_covariance_correlation_and_regression() {
    let sql = "SELECT game_id, covar_pop(amount, length(user_id) * 3 + second(ts)) AS cp, corr(amount, second(ts)) AS r,
                      regr_slope(amount, second(ts)) AS slope, regr_intercept(amount, second(ts)) AS icpt, regr_r2(amount, second(ts)) AS r2,
                      regr_count(amount, second(ts)) AS rn, regr_sxy(amount, second(ts)) AS sxy
               FROM clicks GROUP BY ALL HAVING regr_count(amount, second(ts)) > 3";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
    // Constant y per group.
    check_view("SELECT amount, corr(amount, second(ts)) AS r, regr_r2(amount, second(ts)) AS r2, stddev(amount) AS sd FROM clicks GROUP BY ALL", &EPOCHS, true);
}

#[test]
fn global_covariance_over_retractions() {
    let sql = "SELECT game_id, count(*) AS n, covar_samp(amount, second(ts)) AS cs, corr(amount, second(ts)) AS r, regr_avgx(amount, second(ts)) AS ax
               FROM changes GROUP BY ALL";
    check_view(sql, &EPOCHS, false);
    check_view(sql, &EPOCHS, true);
}

/// The upsert tests' epochs: the fourth holds only message 45, a tombstone of an order that has a row, so a key
/// is deleted in an epoch without any other message of it.
const UPSERT_EPOCHS: [(i64, i64); 6] = [(0, 7), (7, 40), (40, 45), (45, 46), (46, 200), (200, 260)];

fn upsert(key: &[&str], version: Option<&str>) -> UpsertSpec {
    UpsertSpec { key: key.iter().map(|k| k.to_string()).collect(), version: version.map(String::from) }
}

#[test]
fn upsert_last_message_wins() {
    // Without a version the later message of a key replaces its row, and a tombstone deletes it; keys come from
    // a JSON object key, a plain-text key, or the value when the message has none.
    let all = "SELECT * FROM orders";
    check_upsert(all, &upsert(&["order_id"], None), &UPSERT_EPOCHS, false);
    check_upsert(all, &upsert(&["order_id"], None), &UPSERT_EPOCHS, true);
    // A filter and a projection apply to each key's current row: a row that stops passing leaves the table.
    let paid = "SELECT order_id, status, amount * 2 AS doubled FROM orders WHERE status <> 'new'";
    check_upsert(paid, &upsert(&["order_id"], None), &UPSERT_EPOCHS, false);
    check_upsert(paid, &upsert(&["order_id"], None), &UPSERT_EPOCHS, true);
}

#[test]
fn upsert_with_version() {
    // With a version a message older than the key's row is ignored, of equal versions the later message wins
    // (the harness's versions repeat), a NULL version is the lowest, and a deleted key comes back with its next
    // message whatever its version.
    let sql = "SELECT order_id, status, amount, ver FROM orders";
    check_upsert(sql, &upsert(&["order_id"], Some("ver")), &UPSERT_EPOCHS, false);
    check_upsert(sql, &upsert(&["order_id"], Some("ver")), &UPSERT_EPOCHS, true);
    check_upsert("SELECT order_id, ver, status FROM orders WHERE amount > 3", &upsert(&["order_id"], Some("ver")), &UPSERT_EPOCHS, true);
}

#[test]
fn upsert_with_a_composite_key() {
    // Two key columns: an object key gives both, a plain-text key neither (then the value does).
    check_upsert("SELECT order_id, status, amount FROM orders", &upsert(&["order_id", "status"], None), &UPSERT_EPOCHS, false);
    check_upsert("SELECT order_id, status, ver FROM orders", &upsert(&["order_id", "status"], Some("ver")), &UPSERT_EPOCHS, true);
}

#[test]
fn upsert_with_a_lookup() {
    // Each key's current row looks up a lake table; a LEFT JOIN keeps the rows without a match.
    let sql = "SELECT o.order_id, o.status, g.studio FROM orders o JOIN games g ON g.game_id = 'g' || (o.order_id % 6)";
    check_upsert(sql, &upsert(&["order_id"], None), &UPSERT_EPOCHS, false);
    let left = "SELECT o.order_id, o.ver, g.studio FROM orders o LEFT JOIN games g ON g.game_id = 'g' || (o.order_id % 6)";
    check_upsert(left, &upsert(&["order_id"], Some("ver")), &UPSERT_EPOCHS, true);
}
