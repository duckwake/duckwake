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

**Recipes and guides (2026-11-23):** at the user's request, task docs in RisingWave's shape: `docs/recipes/*.md`
(four end-to-end recipes with fixed data and the exact tables shown) and `docs/guides/*.md` (eight "Work with
data" pages), synced to `/docs/recipes/` and `/docs/data/` (sidebar groups Recipes, and Work with data with
Ingest, Process, Serve, Manage). `scripts/recipes_test.sh` runs the recipes as written in CI; a new recipe needs
its blocks runnable or marked `<!-- test: skip -->`, an expected table after `<!-- test: expect -->`, and its
own topic and view names, dropped at its end. duckwake/duckwake#71 and duckwake.github.io#9.

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
flags on".

**Animations (duckwake.github.io PR #6, merged 2026-11-18):** at the user's request, six scenes built with Motion
(`motion` 14.9.1) in that repository's `src/components/motion/` and `src/scripts/motion/`: the live hero table,
one epoch (`EngineFlow`), a worker killed mid-epoch (`ExactlyOnce`), a view over a lake table's change feed
(`ChangeFeed`), watermarks (`Watermark`) and failover (`Failover`), on the home page and Concepts (now
`concepts.mdx`). Each shows the engine's real behaviour, with sources cited in the `*-data.ts` files: when the
watermark rule, lease timeout, rebalancing, change-feed weights or what an epoch commits change here, change the
scene too. Motion leaves an interrupted animation's `finished` pending forever, so the runtime (`scene.ts`)
settles every wait on its own clock; route all waits through it. Docs synced from this repository stay plain
Markdown (GitHub renders them), so scenes go only on pages written in the website repository. The home page's
use cases (`src/components/UseCases.astro`, PR #7 there) show four real views with their SQL, each shape covered
by a test here (`crates/circuit/src/tests/views.rs`); change them when the SQL surface does.
Related: [[duckwake-design-doc]], [[duckwake-branches-prs]].
