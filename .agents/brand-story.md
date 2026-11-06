# DuckWake — Brand story

Brand-forward and editorial: the reader (a data engineer) is the hero and DuckWake is the guide. There are no founder details yet; add the founder's origin moment here if they want a first-person version.

## Version 1 — The long story (About page)

See `site/about.html`. Arc:
- **Opening scene**: the dashboard is an hour old although the events have been in Kafka for a minute.
- **Problem**: fresh tables seem to require a streaming platform (a cluster, state backends, checkpoints, a second language, a team), or a batch job that loses or duplicates rows when it fails.
- **Villain**: the belief that fresh data needs heavy machinery.
- **Realization**: DuckDB made analytics small, and DuckLake makes a commit a transaction. Put a view's rows, its state and the Kafka offsets in one lake transaction, and exactly-once stops being a distributed-systems problem.
- **Decision**: build the small version.
- **Customer**: data engineers on DuckDB and DuckLake.
- **Commitments**: say what it does, with numbers; publish the limits; keep it small; the lake stays the source of truth.

## Version 2 — The short story (homepage or pitch, ~170 words)

The events reached Kafka a minute ago, but the dashboard shows last night. Keeping a lake table fresh has usually meant one of two things. You run a streaming platform, with a cluster, state backends, checkpoint tuning and a second language next to SQL. Or you schedule a batch job and hope it never fails halfway, because then it loses or duplicates rows.

DuckWake starts from a simpler idea. DuckDB made analytics fit in one process. DuckLake makes every commit a transaction. So DuckWake runs your SQL view in small epochs, and commits each epoch's new rows, the view's state and the Kafka offsets it read in one DuckLake transaction. A crash can't lose output or write it twice, and the result is an ordinary table that any DuckDB client can read.

It is for data engineers who already like DuckDB and want fresh tables without adopting a platform. We say what it does with numbers, and we publish the limits.

## Version 3 — The one-liner origin

DuckWake started because keeping a lake table fresh from Kafka seemed to require a whole streaming platform — and we believed one SQL view and one DuckLake transaction per epoch should be enough.
