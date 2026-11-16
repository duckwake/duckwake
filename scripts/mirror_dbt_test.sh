#!/usr/bin/env bash
# scripts/mirror_dbt.sh against a local bare repository standing in for duckwake/dbt-duckwake:
#   1. layout: the package at the root, only tracked files, LICENSE, no README link left pointing out of dbt/.
#   2. publish: the first push creates main; publishing the same tree again pushes nothing.
#   3. A release tag on the tree main holds points at main's head.
#   4. A release tag on a tree main no longer holds gets a commit of its own on top of main; main stays put.
#   5. Publishing a tag again is a no-op; the same tag over other content is refused.
#   6. dbt/dbt_project.yml carries the workspace version, which the release tags both repositories with.
# Needs only git; no Docker, no build.
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0
ok() { echo "ok: $*"; }
fail() { echo "FAIL: $*"; failed=1; }
# This machine's git settings stay out of it (push negotiation, signing); the remote is a local directory.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.invalid GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.invalid

# Its HEAD names master, not main: a clone then checks nothing out, which publish must cope with.
git init -q --bare --initial-branch master "$work/remote.git"
git clone -q "$work/remote.git" "$work/clone" 2>/dev/null
# An untracked file under dbt/ (a local dbt run leaves target/ behind) must not be published.
junk=dbt/example/target/mirror_test_junk
mkdir -p "$(dirname "$junk")" && touch "$junk"
trap 'rm -rf "$work" "$junk"' EXIT

echo "1. layout"
scripts/mirror_dbt.sh layout "$work/clone" >/dev/null
for f in dbt_project.yml macros/materializations example/dbt_project.yml example/packages.yml LICENSE README.md \
  .github/pull_request_template.md; do
  [[ -e $work/clone/$f ]] || fail "layout has no $f"
done
[[ ! -e $work/clone/example/target ]] && ok "untracked files left out" || fail "untracked $junk was published"
want=$(cd dbt && git ls-files | sort); got=$(cd "$work/clone" && find . -type f ! -path './.git/*' | sed 's#^\./##' |
  grep -v -x -e LICENSE -e .github/pull_request_template.md | sort)
[[ $want == "$got" ]] && ok "the files git tracks under dbt/, plus LICENSE" || { fail "file list differs"; diff <(echo "$want") <(echo "$got") || true; }
grep -q '](\.\./' "$work/clone/README.md" && fail "README links out of dbt/" || ok "README links absolute"
grep -q 'https://github.com/duckwake/duckwake/blob/main/docs/' "$work/clone/README.md" && ok "README links at main" || fail "README links not rewritten"
grep -q 'published from \[`dbt/` in duckwake/duckwake\]' "$work/clone/README.md" && ok "README says where to contribute" || fail "README has no mirror note"
cmp -s <(sed -n '3p' "$work/clone/README.md") <(echo '> This repository is published from [`dbt/` in duckwake/duckwake](https://github.com/duckwake/duckwake/tree/main/dbt), where the package') \
  && ok "the note follows the title" || fail "the note is not under the title"

echo "2. publish to main"
SOURCE_SHA=1111111111111111111111111111111111111111 scripts/mirror_dbt.sh publish "$work/clone"
n=$(git -C "$work/remote.git" rev-list --count main)
[[ $n == 1 ]] && ok "main has one commit" || fail "main has $n commits"
git -C "$work/remote.git" log -1 --format=%B main | grep -q 'duckwake/duckwake@111111111111' && ok "commit names its source" || fail "commit message"
out=$(scripts/mirror_dbt.sh publish "$work/clone")
[[ $out == *"up to date"* && $(git -C "$work/remote.git" rev-list --count main) == 1 ]] && ok "unchanged tree: nothing pushed" || fail "republish: $out"
# A fresh clone, as the workflow makes each time, sees the same.
rm -rf "$work/clone2" && git clone -q "$work/remote.git" "$work/clone2"
scripts/mirror_dbt.sh layout "$work/clone2" >/dev/null
out=$(scripts/mirror_dbt.sh publish "$work/clone2")
[[ $out == *"up to date"* ]] && ok "a fresh clone: nothing pushed" || fail "fresh clone republish: $out"
main1=$(git -C "$work/remote.git" rev-parse main)

echo "3. a tag on main's tree"
scripts/mirror_dbt.sh publish "$work/clone2" v9.9.8 >/dev/null
[[ $(git -C "$work/remote.git" rev-parse 'v9.9.8^{commit}') == "$main1" ]] && ok "tag at main's head" || fail "v9.9.8 is not at main"
[[ $(git -C "$work/remote.git" cat-file -t v9.9.8) == tag ]] && ok "annotated tag" || fail "v9.9.8 is not annotated"

echo "4. a tag on a tree main no longer holds"
rm -rf "$work/clone3" && git clone -q "$work/remote.git" "$work/clone3"
scripts/mirror_dbt.sh layout "$work/clone3" v9.9.9 >/dev/null   # README links at v9.9.9: another tree
scripts/mirror_dbt.sh publish "$work/clone3" v9.9.9 >/dev/null
tagged=$(git -C "$work/remote.git" rev-parse 'v9.9.9^{commit}')
[[ $tagged != "$main1" && $(git -C "$work/remote.git" rev-parse "$tagged^") == "$main1" ]] && ok "tag commit on top of main" || fail "v9.9.9 parent"
[[ $(git -C "$work/remote.git" rev-parse main) == "$main1" ]] && ok "main did not move" || fail "main moved"
git -C "$work/remote.git" show "v9.9.9:README.md" | grep -q 'duckwake/duckwake/blob/v9.9.9/' && ok "tagged README links at the tag" || fail "tagged README links"

echo "5. tags are published once"
out=$(scripts/mirror_dbt.sh publish "$work/clone3" v9.9.9)
[[ $out == *"already published"* ]] && ok "same tag, same tree: no-op" || fail "republish tag: $out"
rm -rf "$work/clone4" && git clone -q "$work/remote.git" "$work/clone4"
scripts/mirror_dbt.sh layout "$work/clone4" >/dev/null
if scripts/mirror_dbt.sh publish "$work/clone4" v9.9.9 2>"$work/err"; then fail "v9.9.9 over other content was accepted"
else grep -q 'already exists with other content' "$work/err" && ok "same tag, other tree: refused" || fail "$(cat "$work/err")"; fi

echo "6. versions"
crate=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
pkg=$(sed -n "s/^version: *['\"]\{0,1\}\([^'\"]*\)['\"]\{0,1\}$/\1/p" dbt/dbt_project.yml)
[[ $crate == "$pkg" ]] && ok "dbt/dbt_project.yml version $pkg = Cargo.toml" || fail "dbt/dbt_project.yml version $pkg, Cargo.toml $crate"

[[ $failed == 0 ]] && echo PASSED || { echo FAILED; exit 1; }
