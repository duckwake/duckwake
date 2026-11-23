#!/usr/bin/env python3
"""Runs the recipes in docs/recipes/ step by step, as a reader would, and checks the output they show.

Every fenced block runs in order: `sql` blocks on the DuckWake server over Quack (`duckwake sql --server`), `sh`
blocks with bash from the repository's root. An HTML comment on the line before a block changes that:

  <!-- test: skip -->     the block is not run (setup the test does itself: the stack, the server, ATTACH)
  <!-- test: expect -->   a `text` block holding DuckDB's table output: the `sql` block before it, a query, is
                          run again until its rows equal the table's rows (it waits for the views to commit),
                          or EXPECT_TIMEOUT seconds pass

A `sql` block that calls duckwake_verify must print `VERIFY OK` once for each call. Other `text` blocks are not
checked. RECIPE_SUBST ("from=>to" per line) rewrites every block before it runs, for a local setup that differs
from the compose stack the recipes use.

Usage: recipes.py docs/recipes/a.md [...]   (BIN, SERVER and EXPECT_TIMEOUT from the environment)
"""
import csv
import io
import os
import re
import subprocess
import sys
import time

BIN = os.environ.get("BIN", "target/release/duckwake")
SERVER = os.environ.get("SERVER", "quack:localhost:9494")
EXPECT_TIMEOUT = float(os.environ.get("EXPECT_TIMEOUT", "120"))
SUBST = [tuple(l.split("=>", 1)) for l in os.environ.get("RECIPE_SUBST", "").splitlines() if "=>" in l]
MARKER = re.compile(r"^<!--\s*test:\s*(skip|expect)\s*-->\s*$")
FENCE = re.compile(r"^(```+)(\w*)\s*$")


def blocks(path):
    """(line, lang, body, marker) for each fenced block of the file."""
    out, marker, lines = [], None, open(path, encoding="utf-8").read().split("\n")
    i = 0
    while i < len(lines):
        m = MARKER.match(lines[i])
        if m:
            marker = m.group(1)
            i += 1
            continue
        f = FENCE.match(lines[i])
        if f:
            start, body, i = i + 1, [], i + 1
            while i < len(lines) and not lines[i].startswith(f.group(1)):
                body.append(lines[i])
                i += 1
            out.append((start, f.group(2), "\n".join(body), marker))
            marker = None
        elif lines[i].strip():
            marker = None  # a marker applies to the block right after it
        i += 1
    return out


def rewrite(body):
    for a, b in SUBST:
        body = body.replace(a, b)
    return body


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def sql(body, csv_out=False):
    return run([BIN, "sql", "--server", SERVER] + (["--csv"] if csv_out else []) + [body])


def table_rows(text):
    """The data rows of a DuckDB box table."""
    rows, data = [], False
    for line in text.split("\n"):
        if line.startswith("├"):
            data = True
        elif line.startswith("└"):
            break
        elif data and line.startswith("│"):
            rows.append([c.strip() for c in line.strip().strip("│").split("│")])
    return rows


def csv_rows(text):
    return [["NULL" if c == "" else c for c in r] for r in csv.reader(io.StringIO(text)) if r]


def same(a, b):
    if a == b:
        return True
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    return abs(x - y) <= 1e-9 * max(1.0, abs(x), abs(y))


def equal(want, got):
    return len(want) == len(got) and all(
        len(w) == len(g) and all(same(x, y) for x, y in zip(w, g)) for w, g in zip(want, got)
    )


def show(rows):
    return "\n".join("  " + " | ".join(r) for r in rows) or "  (no rows)"


def recipe(path):
    steps = blocks(path)
    print(f"== {path}: {len(steps)} blocks")
    for n, (line, lang, body, marker) in enumerate(steps):
        nxt = steps[n + 1] if n + 1 < len(steps) else None
        where = f"{path}:{line}"
        if marker == "skip" or (lang == "text" and marker != "expect"):
            continue
        body = rewrite(body)
        if lang == "sql" and nxt and nxt[3] == "expect":
            continue  # run by the expect block below, until it matches
        if lang == "text":
            query = rewrite(steps[n - 1][2]) if n and steps[n - 1][1] == "sql" else None
            if query is None:
                return f"{where}: an expected table must follow a sql block"
            want, got, err = table_rows(body), None, ""
            deadline = time.time() + EXPECT_TIMEOUT
            while True:
                r = sql(query, csv_out=True)
                if r.returncode == 0:
                    got = csv_rows(r.stdout)
                    if equal(want, got):
                        break
                else:
                    err = r.stderr.strip()
                if time.time() > deadline:
                    return (f"{where}: after {EXPECT_TIMEOUT:.0f}s the query\n{query}\nreturned\n"
                            f"{show(got or [])}\nnot\n{show(want)}" + (f"\nlast error: {err}" if err else ""))
                time.sleep(1)
            print(f"  {where}: {len(want)} rows as shown")
        elif lang == "sql":
            r = sql(body)
            if r.returncode != 0:
                return f"{where}: the SQL failed:\n{body}\n{r.stderr.strip()}"
            calls, oks = body.count("duckwake_verify("), r.stdout.count("VERIFY OK")
            if calls and (oks != calls or "VERIFY FAILED" in r.stdout):
                return f"{where}: {oks} of {calls} checks passed:\n{r.stdout}"
            print(f"  {where}: sql ok" + (f", {oks} VERIFY OK" if calls else ""))
        elif lang == "sh":
            r = run(["bash", "-euo", "pipefail", "-c", body])
            if r.returncode != 0:
                return f"{where}: the command failed:\n{body}\n{r.stdout}{r.stderr}"
            print(f"  {where}: sh ok")
        else:
            return f"{where}: no rule to run a `{lang}` block; mark it <!-- test: skip -->"
    return None


def main():
    failed = 0
    for path in sys.argv[1:]:
        err = recipe(path)
        if err:
            print(f"FAIL {err}")
            failed = 1
    sys.exit(failed)


if __name__ == "__main__":
    main()
