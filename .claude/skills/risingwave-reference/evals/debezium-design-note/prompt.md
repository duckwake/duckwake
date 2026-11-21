---
tags: [skill-creator]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Bash, WebSearch, WebFetch]
---

Mình sắp làm C3 (#42), đọc Debezium topics trong DuckWake. Viết giúp mình một design note ngắn (1–2 trang) vào file design-note.md: Debezium envelope (before/after/op) đi vào engine thế nào, snapshot (op = r), delete và tombstone, key không tồn tại, cấu hình trên source hay view, và test/gate. Có so sánh với RisingWave để mình biết nên lấy gì.
