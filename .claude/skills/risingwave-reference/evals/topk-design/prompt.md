---
tags: [skill-creator]
max_turns: 40
timeout_seconds: 1200
allowed_tools: [Read, Glob, Grep, Skill, Write, Bash, WebSearch, WebFetch]
---

Đề xuất cách DuckWake làm top-k theo nhóm (issue #35), kiểu SELECT ... FROM (SELECT *, row_number() OVER (PARTITION BY game_id ORDER BY amount DESC) AS rn FROM clicks) WHERE rn <= 3. RisingWave làm thế nào, DuckWake nên làm thế nào cho đơn giản mà vẫn exactly-once và verify được? Viết vào topk.md.
