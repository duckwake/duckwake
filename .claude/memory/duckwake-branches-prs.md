---
name: duckwake-branches-prs
description: Where the work stands in git (as of 2026-11-15) - everything merged into main of the public duckwake/duckwake; how new work is branched, checked and merged
metadata:
  type: project
---

`duckwake/duckwake` is public (MIT) since 2026-11-15, which made GitHub Actions free; the private repository ran
out of Actions minutes that morning. `main` holds all work, merged with merge commits:

1. PR #1: open-source release files (license, notices, design docs, community files, license CI).
2. PR #3: `rebalance_test.sh` waits for load moves to settle instead of a fixed window.
3. PR #5: a Kafka reader gives up after 5 s waiting for its topic's partitions (it had hung the worker loop).
4. PR #2: F3, backfill and retention of views over lake inputs, with three engine fixes found by its CI (each run
   of a view stages into its own tables; a dropped view no longer stops its worker; a failed verify rolls back).
5. PR #4: dbt-duckwake in `dbt/` (dbt plan step 3).
6. 2026-11-15/16: PR #7 (pull Docker Hub images once per CI run), #8 (pinned Redpanda and MinIO), #9 (a worker
   survives a view dropped while it starts it), #10 (zero-downtime replace on the roadmap), #12 (a lease is taken
   only while the view runs), #13 (more aggregates: variances, stddev, `count(DISTINCT)`, `bool_and`/`bool_or`,
   windowed `HAVING`), #15 (a worker survives a view dropped while its epoch stages), #14 (SQL reference
   restructured), #11 (release copies extensions into the website's `public/`), #16 (dbt/ mirrored to
   duckwake/dbt-duckwake). PR #6 carries this memory.
7. 2026-11-16: PR #17 released **v0.2.0** (tag on its merge commit `9e7fe99`): binaries for 4 platforms, the
   extension repository (also on the website under `/extensions`, `PUBLISH_EXTENSIONS=true` with `PAGES_TOKEN`),
   the image `ghcr.io/duckwake/duckwake` (public since that day) and dbt-duckwake's `v0.2.0`. A manual run of the
   release workflow (Actions -> release -> Run workflow, on the release branch) first caught that `macos-14`
   runners were retired on 2026-11-02 (now `macos-15`); run one before every tag, since `v*` tags cannot move.
   The version bump also needs `THIRD_PARTY_LICENSES.md` regenerated (`cargo about`). PR #18 makes release notes
   link to files at the tag.
8. 2026-11-17: the roadmap became GitHub issues #20-#55 (label `roadmap` plus a category label: `sql`,
   `sources`, `dbt`, `ops`, `engine`, `distribution`; issue types Feature/Task/Bug; joins and CDC have
   sub-issues); the user builds the Projects board from them, not stage labels like P3. PR #56 added
   quantiles, `approx_count_distinct`, `covar`/`corr`/`regr_*` and windowed `count(DISTINCT)`; PR #57 the
   community-extension build (root `Makefile`, `test/sql`) and a Redpanda port fix. DuckWake became a DuckDB
   community extension (duckdb/community-extensions#3654, from the fork `duckwake/community-extensions`,
   branch `duckwake`, maintainer harrydevforlife) at commit `16ec8d6`; each release moves its `ref` there.
   The fork's macOS jobs never got a runner (cancelled after 15 min queued); DuckDB's own CI runs them.
9. 2026-11-21: PR #60 (C2, #41: upsert views, `upsert_key`/`upsert_version`), PR #61 (two intermittent CI
   failures, both engine/test bugs already on main: a windowed view over another view's windows waited up to
   `position_refresh` once the upstream went idle, now it records its watermark at least once a slide; and
   `verify` rounded floats, now compares them within 1e-9) and PR #62 (C3, #42 closed: Debezium sources, gates
   `FORMAT=debezium scripts/upsert_test.sh` and `scripts/debezium_e2e_test.sh` with PostgreSQL and Debezium 3.6.3
   in compose profile `debezium`; CI reserves the stack's host ports). Branch `skill-risingwave-reference`
   (the RisingWave reference skill, `.claude/skills/`) is pushed without a PR, at the user's request; it is not
   on main yet. Next on the roadmap: C4 (#38), windows over updates and deletes.

Before the move (2026-11-14) the history was rewritten with `git filter-repo` (the author's work email replaced
by their personal one, claude.ai links replaced) and published fresh; PR numbers of the old repository
(`duckwake-archive`, private) do not exist here. One-time, user-approved exception to the no-force-push rule.

**Why:** the user wants each piece of work on its own branch with its own PR, and decides when to merge.

**How to apply:** start new work on a new branch from `main` (or stacked on an open branch), with its own draft
PR and green CI. The user imported rulesets: `main` takes changes only through PRs, `ci-passed` must be green
(so no `[skip ci]` on a PR's head; GitHub skips the run when that marker appears anywhere in the head commit's
message, even quoted, so never write it into a commit message), merge commits only, no force-push; `v*` tags cannot move or be deleted. Merge
only when the user asks, retargeting each next PR to `main` first; merge `main` into a stacked branch, never
rebase. Commit messages carry the Co-Authored-By line but no Claude-Session link (the user removed claude.ai
links from the history). PR descriptions end with the plain "Generated with Claude Code" line; the PR tool can
append its own footer with a claude.ai session link, so read the body back after creating or editing a PR and
replace it (it did on #9-#12 and duckwake.github.io#1, 2026-11-15). A red CI job gets its root cause, not a
re-run: five "flakes" on 2026-11-15 were real engine bugs (the last two: a worker exiting when a view it was
starting was dropped, PR #9; a paused view taking a fresh lease, PR #12). Related: [[duckwake-working-rules]], [[duckwake-testing]].
