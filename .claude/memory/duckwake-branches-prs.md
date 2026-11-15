---
name: duckwake-branches-prs
description: Where the work stands in git (as of 2026-11-14) - PRs #3, #4, #5 merged into main; how new work is branched and merged
metadata:
  type: project
---

On 2026-11-14 the user asked to merge everything, and `main` now holds all work so far, merged with merge
commits in stack order:

1. PR #3 (`ci-suites`): CI for every suite, failover, rebalancing, Kafka auth, S3, lake inputs (P1, P2).
2. PR #4 (`p4-operations`): schema changes, limits (`--threads`), monitoring, releases (P4), and the roadmap
   change to dbt authoring.
3. PR #5 (`create-view-if-exists`): `create_view(..., if_exists := 'fail'|'update'|'replace')` with the
   outcome as the result's first word (dbt-duckwake plan step 1), plus `CLAUDE.md` and `.claude/memory/`.

In the new repository: PR #1 (open-source release files) and PR #3 (rebalance test settle fix) are merged;
PR #2 (`f3-backfill`, F3 backfill and retention) targets `main`; the dbt package is on `dbt-duckwake`, stacked
on `f3-backfill`. See [[duckwake-dbt-not-ddl]].

On 2026-11-14, before open-sourcing (MIT), the history was rewritten with `git filter-repo` (the author's work
email replaced by their personal one, claude.ai links replaced) and published to a fresh `duckwake/duckwake`
repository. The PR numbers above refer to the old repository; they do not exist in the new one, and every commit
hash changed. This was a one-time, user-approved exception to the no-force-push rule.

**Why:** the user wants each piece of work on its own branch with its own PR, and decides when to merge.

**How to apply:** start new work on a new branch from `main` (or stacked on an open branch), with its own
draft PR and green CI. Merge only when the user asks; then use merge commits (`gh pr merge --merge`), which
keep stacked branches intact, retargeting each next PR to `main` before merging it. Never force-push.
Related: [[duckwake-working-rules]].
