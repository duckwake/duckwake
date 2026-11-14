---
name: duckwake-branches-prs
description: Where the work stands in git (as of 2026-11-14) - stacked draft PRs #3 and #4, and the create-view-if-exists branch
metadata:
  type: project
---

As of 2026-11-14 the work is a stack of branches, each its own draft PR, none merged into `main`:

1. PR #3, branch `ci-suites`: CI for every suite, failover, rebalancing, Kafka auth, S3, lake inputs (P1, P2).
2. PR #4, branch `p4-operations`, on top of #3: schema changes, limits (`--threads`), monitoring, releases
   (P4), and the roadmap change to dbt authoring. CI green (29 checks, both limits suites).
3. Branch `create-view-if-exists`, on top of #4: `create_view(..., if_exists := 'fail'|'update'|'replace')`
   with the outcome as the result's first word (dbt-duckwake plan step 1). Built and tested locally
   (`schema_test.sh`, `serve_test.sh`, unit tests), not yet committed; the user has not yet said whether to
   commit it and open a PR.

**Why:** the user asked for P4 "on a new branch with its own PR", and the dbt plan says each step is its own PR.

**How to apply:** new work goes on a new branch stacked on the latest one, with its own draft PR. Merging is
the user's call; never `gh pr merge` or force-push. Related: [[duckwake-working-rules]], [[duckwake-dbt-not-ddl]].
