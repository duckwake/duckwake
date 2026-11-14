---
name: Bug report
about: Something DuckWake does wrong
labels: bug
---

**What happened**

**What you expected**

**How to reproduce**
The SQL (`duckwake_create_source`, `duckwake_create_view`, …) or the steps, and a few input events if they matter.

**Environment**
- `duckwake --version`:
- Front-end: `duckwaked` / `LOAD duckwake` / `duckwake` CLI
- Catalog (PostgreSQL version), Kafka (or Redpanda) version, data path (local or S3):
- OS and architecture:

**Logs**
The worker log around the failure (`RUST_LOG=info`), and `FROM duckwake_jobs()` if a view is involved.
