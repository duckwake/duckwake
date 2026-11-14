# Performance: worker flags and benchmarks

`run`, `serve` (`duckwaked`) and `playground` take four flags from the
[parallel processing plan](design/parallel-processing.md). They are off by
default until a 24 h soak with them on has passed:
- `--stage-threads N` stages views in parallel, each thread on its own DuckDB connection to the same
  instance (`0` = half the cores).
- `--pipeline` stages the next epoch while the previous one commits, on a thread and connection of its own.
  Commits stay in order and at most one epoch is staged ahead. A global aggregate reads the previous
  epoch's new state from memory until that epoch has committed.
- `--shared-readers` gives the views of one source that are at the same position one Kafka reader, one
  decode and one staged copy of the events. A view at another position (new, reset, or behind) reads on
  its own until it reaches the shared reader's position, then joins it.
- `--lean-commit` skips the epoch transaction's writes that have nothing to write (no window closing, no
  late rows, no dead letters, no changed groups), and writes all views' checkpoints and dead letters with
  one statement each.

## Measured (MacBook, Apple Silicon, 10 cores)

A backlog of 300k events, the playground's mix of views, one worker (`CATCHUP=1 scripts/scale_bench.sh`):

| Views | Flags off | All four flags | Gain |
| --- | --- | --- | --- |
| 1 | 106k events/s | 108k events/s | 1.0× |
| 4 | 62k events/s | 82k events/s | 1.3× |
| 16 | 21.7k events/s | 40.5k events/s | 1.87× |

16 views at a steady 20k events/s with 2 s epochs, each flag added to the previous ones (medians):

| Per epoch | Flags off | + parallel staging | + pipeline | + shared readers | + lean commit |
| --- | --- | --- | --- | --- | --- |
| Read + stage | 1,552 ms | 512 ms | 529 ms | 245 ms | 297 ms |
| Commit | 662 ms | 571 ms | 594 ms | 494 ms | 327 ms |
| Worker CPU | 89% | 101% | 102% | 48% | 46% |
| Peak memory | 495 MB | 495 MB | 503 MB | 319 MB | 341 MB |

## Benchmark

`scripts/scale_bench.sh` runs 1, 2, 4, 8 and 16 views (the playground's mix) on one worker at a fixed
rate, or with `CATCHUP=1` through a backlog, and appends epoch period, freshness, stage and commit time
(statements vs `COMMIT`), CPU and memory to `.lake/bench/*.csv`; `FLAGS` and `LABEL` name the variant.
