---
name: duckwake-production-grade
description: DuckWake is being taken to production; implement every roadmap item to production grade
metadata:
  type: project
---

As of 2026-11-12 the user said DuckWake is going to production and asked that work be implemented production grade.

**Why:** the roadmap (P1 CI, P2 real environment, P3 load/failure, P4 operable) is the path to a production
launch; shortcuts acceptable in early prototypes are not acceptable. Docs and code never call DuckWake a proof of concept.

**How to apply:** for each feature, cover restarts, partial failures, concurrency between workers, ops controls
(flags, CLI/SQL, metrics, logs), docs (`docs/*.md`, CHANGELOG), unit tests for pure logic, and an integration gate
script that runs in CI; verify on Linux CI, not only macOS. Prefer simple, well-tested designs over clever ones.
Related: [[duckwake-testing]], [[duckwake-branches-prs]].
