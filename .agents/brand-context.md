# Brand Context

## Brand
- **Name**: DuckWake
- **Category**: Open-source data infrastructure / developer tool (stream processing, lakehouse)
- **Description**: Continuous SQL over Kafka into DuckLake, exactly once. You write a view in SQL; DuckWake keeps a DuckLake table up to date with it as events arrive.
- **Stage**: Pre-launch (proof of concept, built on DuckDB 2.0 and DuckLake 1.1)
- **Website**: N/A (GitHub repo planned; org name not decided)

## Audience
- **Primary Audience**: Data engineers and analytics engineers already using DuckDB and DuckLake (or evaluating a lakehouse) who need tables kept fresh from Kafka, at small-to-mid scale.
- **Key Problem**: Keeping lake tables fresh from event streams today means running and operating a streaming cluster (Flink, Spark Structured Streaming, Kafka Connect + cron jobs), with hard-to-get exactly-once guarantees and a second language besides SQL.
- **Their Language**: "exactly once", "late events", "watermark", "freshness", "lag", "backfill", "just SQL", "no cluster", "materialized view", "CDC", "lakehouse", "it just keeps the table up to date"

## Positioning
- **Differentiation**: Plain SQL views; one process next to DuckDB, no streaming cluster; every epoch commits output rows, view state and Kafka offsets in one DuckLake transaction, so a crash never loses or duplicates output; incremental computation on a DBSP-style operator graph (Feldera's model).
- **Competitors**: Apache Flink (and Flink SQL), Materialize, RisingWave, Feldera; indirectly Spark Structured Streaming, Arroyo, Kafka Connect + scheduled batch jobs.
- **Market Position**: Niche — the lightweight, DuckDB-native option for streaming into the lake.

## Brand Personality
- **Personality Words**: precise, light, candid
- **Not**: hype, heavy enterprise, overly cute
- **Tone**: casual-professional, serious about correctness, never playful at the expense of clarity
- **Voice Admires**: DuckDB (plain, technical, a little dry humour), Tailscale and Fly.io docs (direct, concrete)

## Values & Mission
- **Core Values** (working assumption): correctness you can verify; simplicity over machinery; say what it does, with numbers; honest limits ("not supported yet", with the reason)
- **Mission** (draft): Make a fresh lake table as easy as writing a SQL view — exactly once, without a streaming cluster.

## Goals
- **Primary Goal** (working assumption): Publish the open-source project and get first users from the DuckDB/DuckLake community.
- **Key Metrics** (working assumption): GitHub stars, first external users running it, issues/PRs from outside contributors.

## Brand constraints
- Do not use DuckDB's duck logo or its signature yellow; say "built on DuckDB" in words. DuckDB and DuckLake are trademarks of the DuckDB Foundation.
- Existing palette (brand workbook): Ink #0E1A24, Paper #F7F8F5, Lake #1D5A7A, Wake #2FA39A / Wake Deep #1E7A73, Amber #F4B728.
- Existing type: Space Grotesk (display), Inter (text), JetBrains Mono (code).
- Logo chosen (2026-11-06): B3 "Carved, trailing wake" on board 2d — a solid duck in side view with a 45° tail, two wake slits carved through the body that run on behind it as streaks; Paper duck with Amber bill on an Ink tile. A small-size cut (one wide slit, larger eye) is used at 32 px and below.
- Rejected (2026-11-06): the "identity v2" direction in `brand-identity.md` (green palette, Newsreader serif, top-down duck F2).
