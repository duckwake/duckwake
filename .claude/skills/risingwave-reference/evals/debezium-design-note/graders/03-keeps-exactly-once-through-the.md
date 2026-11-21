---
type: llm
focus: trace
weight: 1
---

design-note.md keeps exactly-once through the epoch's single DuckLake transaction (offsets, state and output committed together) and adds no external connector or service
