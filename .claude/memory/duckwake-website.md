The website https://duckwake.github.io went live on 2026-11-15 from `duckwake/duckwake.github.io` (PR #1 there):
Astro + Starlight (versions pinned at least two weeks old), with the brand kit in that repository's `brand/`
(`brand/GUIDELINES.md`: Ink, Paper, Amber, Lake; Space Grotesk, Inter, JetBrains Mono).

**How it fits together:** the home page, About, Install, Quick start and Concepts are written in that
repository. The reference docs, `dbt/README.md`, `CHANGELOG.md` and `docs/design/` stay in this repository and
are copied in at build time by its `scripts/sync-docs.mjs` (each first heading becomes the title, relative links
are rewritten, an edit link points back here). `.github/workflows/pages.yml` there builds every PR (a broken
internal link fails it) and deploys `main` on push, daily and on demand. The release workflow here copies the
extension repository into its `public/extensions/` (PR #11).

**Why:** the user wanted the site's code in the Pages repository and the brand on the docs, while docs keep
being reviewed next to the code they describe.

**How to apply:** a docs change goes in this repository's `docs/`; a new page there needs an entry in
`sync-docs.mjs`'s map and in `astro.config.mjs`'s sidebar. Check claims on the site's own pages against the code
(the Concepts page first said 2 s for the default epoch interval; it is 10 s). This session's proxy cannot reach
the live site; the user checks it in a browser. Related: [[duckwake-design-doc]], [[duckwake-branches-prs]].
