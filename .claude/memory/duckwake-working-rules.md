---
name: duckwake-working-rules
description: What needs the user's go-ahead in this repo (commits, pushes, merges, publishing) and files never to commit
metadata:
  type: feedback
---

- Commit or push only when the user asks; if on `main`, branch first. Commits end with the Co-Authored-By line
  for Claude, PR bodies with the Claude Code line.
- Never commit `notes/state_manipulate.py` (untracked on purpose).
- Force-push, `gh pr merge` and creating public repositories need the user. Merging PRs is the user's call.
- Making the `duckwaked` image or the extension public (ghcr.io visibility, `PUBLISH_EXTENSIONS`) is the
  organization's call.
- Do not run externally downloaded binaries (the permission classifier refused node_exporter); do not work
  around such a refusal.
- The user's email is for identification only; never send it to any service or write it into the repo.
- Write docs, commit messages and replies in plain, specific English: no filler, real numbers from real runs.

**Why:** the user set these over the project; they protect shared history and things outside the repo.

**How to apply:** finish the work, report it, and ask before any of the actions above. Related:
[[duckwake-branches-prs]].
