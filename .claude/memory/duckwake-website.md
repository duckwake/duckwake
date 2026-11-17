The website https://duckwake.github.io went live on 2026-11-15 from `duckwake/duckwake.github.io` (PR #1 there):
Astro + Starlight (versions pinned at least two weeks old), with the brand kit in that repository's `brand/`
(`brand/GUIDELINES.md`: Ink, Paper, Amber, Lake; Space Grotesk, Inter, JetBrains Mono).

**How it fits together:** the home page, About, Install, Quick start and Concepts are written in that
repository. The user docs (`docs/*.md` but `internals.md` and `testing.md`), `dbt/README.md` and `CHANGELOG.md`
stay in this repository and are copied in at build time by its `scripts/sync-docs.mjs`. On 2026-11-16 the user
had the design documents, internals and testing taken off the site: they are for people working on DuckWake and
stay on GitHub, and links to them point there (each first heading becomes the title, relative links
are rewritten, an edit link points back here). `.github/workflows/pages.yml` there builds every PR (a broken
internal link fails it) and deploys `main` on push, daily and on demand. The release workflow here copies the
extension repository into its `public/extensions/` (PR #11).

**Why:** the user wanted the site's code in the Pages repository and the brand on the docs, while docs keep
being reviewed next to the code they describe.

**How to apply:** a docs change goes in this repository's `docs/`; a new page there needs an entry in
`sync-docs.mjs`'s map and in `astro.config.mjs`'s sidebar. Check claims on the site's own pages against the code
(the Concepts page first said 2 s for the default epoch interval; it is 10 s). This session's proxy cannot reach
the live site; the user checks it in a browser.

**Positioning (2026-11-16):** `brand/POSITIONING.md` in the website repository is the source for how DuckWake is
described: tagline "Real-time DuckLake tables, exactly once.", the buyer (data engineers on DuckDB and DuckLake
with Kafka), three pillars (your lake is the result; exactly once, and you can check it; nothing new to run), the
home page copy checked claim by claim, and the words not to use ("streaming database", "production-grade",
"sub-second", "any SQL"). Built with the pmm-positioning and product-marketing-toolkit skills; the buyer's interest
is still a hypothesis until users are interviewed. Benchmark numbers always carry the machine and "four worker
flags on". Related: [[duckwake-design-doc]], [[duckwake-branches-prs]].
