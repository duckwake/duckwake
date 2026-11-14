# DuckWake — Competitor analysis and visual identity brief

> Status (2026-11-06): the user rejected the Part B direction (green palette, Newsreader, top-down duck) and chose logo B3 from board 2d. Part A (competitor research) still holds.

Sources: live homepages checked 2026-11-06 (Flink, Materialize, RisingWave, Feldera, Arroyo, Confluent, DuckDB, MotherDuck). Raw captures in the session scratchpad `research/`.

## Part A — Competitor branding (competitor-branding)

### Landscape
Streaming brands look alike: blue or purple on dark heroes (Materialize, Arroyo, Confluent, RisingWave's panels), grotesk/geometric sans with Inter for text, glows and 3D isometric icons. In 2026 almost every hero has pivoted to AI agents ("live context", "intelligence at the speed of your data", "always current"). The DuckDB family owns yellow (DuckDB #FFF100, duck-head circle) and orange (MotherDuck #FF9538, webbed footprints). Feldera, the closest technical peer (DBSP), owns warm paper, olive and a black woodcut bird.

| Brand | Owns | Visual | Voice |
|---|---|---|---|
| Flink | "stateful computations over data streams" | squirrel mascot, purple–pink gradient | community, technical |
| Materialize | "live context layer for agents" | purple #7F4EFF on near-black, abstract striped M | sleek, enterprise-AI |
| RisingWave | "real-time compute for agentic AI" | blue #005EEC wave curl, mint accents | ambitious, AI-forward |
| Feldera | "compute only what changed" | black bird, paper + olive + violet, DM Sans | precise, editorial |
| Arroyo | "SQL stream processing, no ops team" | circle of blue wave stripes, dark | developer-first, scrappy |
| Confluent | "data streaming platform" (IBM) | navy starburst, Poppins | corporate |
| DuckDB | "universal data wrangling tool" | yellow duck head, Swiss grotesk | friendly, pragmatic |
| MotherDuck | "your AI data team" | orange footprints, cream + sky blue, neo-brutalist | playful, witty |

### Category clichés to avoid
- Claims: "real-time", "sub-second", "AI agents / agentic", "live context", "unified", "at any scale".
- Visuals: blue/purple on black, gradient glows, 3D isometric cubes, wave curls and striped wave circles (RisingWave, Arroyo), yellow or orange ducks (DuckDB family).

### Open territory
- Calm, verifiable correctness as the hero claim ("exactly once" you can check: replay, snapshots, time travel).
- "Small, no cluster" on the streaming side.
- Green and natural palettes; light, print-like layouts; a serif-led editorial voice in type.

## Part B — Visual identity brief (brand-identity)

### 01 Identity strategy
DuckWake should look like the calm one in a loud category: a light page, a deep pond green, one warm accent, and type that reads like a well-edited manual. Every visual says "this table is exactly right" rather than "this is fast". The duck and its wake are the one image the brand repeats: something passes over the water and leaves an orderly trace, which is what an epoch does to a table.

### 02 Logo direction
- **Primary**: symbol + wordmark. The symbol is a duck seen from above swimming diagonally, with the Kelvin wake it makes (two diverging arms and transverse arcs). Board 2, option F2.
- **Character**: calm motion, precise geometry, a little warmth from the bill.
- **Style**: constructed from circles and straight lines; flat colour; wings and eye as negative space; a small-size cut without wings and arcs.
- **Avoid**: duck heads or footprints (DuckDB, MotherDuck), wave curls, striped circles, gradients, 3D, glows.
- **References**: Feldera's bird (one figure, no decoration) — but ours is geometric, not woodcut; Twitter's old bird (construction from circles); Swift (motion carried by the silhouette alone).

### 03 Colour
| Name | Hex | Role |
|---|---|---|
| Pond | #0D1F19 | ink: text on light, dark backgrounds, app tile |
| Drake | #135C46 | primary brand green (a mallard's head): headings, links, buttons on light |
| Paper | #F6F7F3 | light background (cool, not Feldera's warm paper) |
| Ripple | #8CCFB0 | the wake: graphics on dark, highlights on Pond (9.5:1) |
| Bill | #F4B728 | single accent: the duck's bill, one key highlight per surface |
| Mist | #5E6B65 | secondary text on Paper (5.2:1) |
Rules: Bill and Ripple only on Pond or Drake, never as text on Paper. No blue, no purple, no gradients. Palette type: analogous greens + one complementary warm accent.

### 04 Typography
- **Display**: Newsreader (serif, Google Fonts), weights 500–600, tight leading. Calm and editorial; no one else in streaming leads with a serif.
- **Text**: Instrument Sans (Google Fonts), 400–600. Neutral, slightly narrow, not Inter.
- **Code**: JetBrains Mono. SQL is the product; code blocks use the palette (keywords Ripple, strings Bill, comments Mist on Pond).
- **Avoid**: geometric display sans that every competitor uses (Space Grotesk, Poppins, Monument Grotesk), all-caps headings, gradient text.

### 05 Imagery
No photos, no 3D. Thin-ruled diagrams of epochs, snapshots and tables; real SQL and terminal output; the wake motif as a quiet pattern (transverse arcs) at low contrast.

### 06 Iconography
Line icons, 1.75 px at 24 px, round caps, Pond or Drake; functional, never decorative.

### 07 Design principles
- **Show the guarantee**: prefer a diagram or a number that proves correctness over an adjective.
- **Calm over loud**: light pages, generous space, one accent per surface.
- **The wake is the motif**: reuse the arms and arcs of the wake as dividers and patterns, never as decoration for its own sake.
- **Code is first-class**: every page shows real SQL.

### 08 Expressions
- **GitHub README / social card**: Pond background, F2 symbol, Newsreader headline "Fresh lake tables. Exactly once.", one SQL snippet.
- **Docs site**: Paper background, Drake links, Newsreader headings, Instrument Sans body, mono code on Pond.
- **CLI banner**: wordmark in Drake/Ripple, epoch lines in Mist.
- **Slides**: Paper slides with one Pond slide per section; diagrams in thin rules.
- **Stickers**: the duck and wake in Paper on Drake.
