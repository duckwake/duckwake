# DuckWake: notes for Claude

Project memory lives in `.claude/memory/`, one fact per file. These files are imported below so every session
starts with them. When something durable changes (a decision, where work stands, a new rule), update the file
that covers it instead of adding a duplicate, and keep this index in step.

- [DuckWake goes to production](.claude/memory/duckwake-production-grade.md): every item production grade, with failures, ops controls and CI tests
- [P3 waits for a VPS](.claude/memory/duckwake-p3-vps.md): P3 starts when the user has a VPS; spec and open items
- [Branches and PRs](.claude/memory/duckwake-branches-prs.md): everything merged into public main; rulesets; how new work is branched and merged
- [dbt, not DDL](.claude/memory/duckwake-dbt-not-ddl.md): SQL authoring through dbt-duckwake; DDL is long term; the plan
- [Working rules](.claude/memory/duckwake-working-rules.md): what needs the user's go-ahead; files never to commit
- [Design doc](.claude/memory/duckwake-design-doc.md): the Claude Doc and its tabs; keep it in step with docs/roadmap.md
- [Testing](.claude/memory/duckwake-testing.md): cargo commands, which binary has the SQL functions, ports, test lake
- [Website](.claude/memory/duckwake-website.md): duckwake.github.io on Starlight; which docs live where; how they sync; the Motion scenes and use cases

@.claude/memory/duckwake-production-grade.md
@.claude/memory/duckwake-p3-vps.md
@.claude/memory/duckwake-branches-prs.md
@.claude/memory/duckwake-dbt-not-ddl.md
@.claude/memory/duckwake-working-rules.md
@.claude/memory/duckwake-design-doc.md
@.claude/memory/duckwake-testing.md
@.claude/memory/duckwake-website.md
