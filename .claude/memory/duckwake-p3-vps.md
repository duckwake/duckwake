---
name: duckwake-p3-vps
description: P3 (soak, failure drills, upgrades, sizing) is on hold until the user rents a VPS; the recommended spec and open items
metadata:
  type: project
---

As of 2026-11-12, P2 is complete. The user will start P3 once they have rented a VPS and will say so.

**Why:** the P3 soak and sizing need a long-running Linux box, not the MacBook's Docker VM.

**How to apply:** when the user brings the VPS, ask for the provider and setup, then prepare the deployment:
compose overrides, systemd units with MemoryMax, soak settings with time-based topic retention, Prometheus.
Recommended spec: x86, 8 dedicated vCPU, 16–32 GB RAM, at least 250 GB local NVMe, an S3 bucket in the same
region. The alternative is two boxes: infra with 4 vCPU / 16 GB, and the worker box as the 4 vCPU / 16 GB
reference machine. With 16 GB: Redpanda `--memory 3G`, workers `--memory-limit 1GB` (and `--threads` low enough,
since DuckDB's memory need grows with threads), Arrow decode, no Grafana on the box.

Open items to carry into P3: a view whose Kafka brokers are unreachable for 10 s when its worker starts it
is marked failed and must be started by hand (a 0.2.0 known issue; the broker-restart drill); resuming views from
0.1.0 checkpoints after an upgrade is untested; a worker exits when a heartbeat write fails (PostgreSQL-down drill); no
`rebalance --dry-run`; tune the alert thresholds in `deploy/prometheus` with the soak's numbers. The P4 items
not needing a VPS (schema changes, limits, alerts/dashboard, distribution) are built; the runbook is left for
after P3's drills. Related: [[duckwake-production-grade]], [[duckwake-branches-prs]].
