#!/usr/bin/env bash
# dbt-duckwake is written in dbt/ of this repository and published as the root of duckwake/dbt-duckwake, where
# dbt Hub and `packages: - git:` find it (both need dbt_project.yml at a repository's root). This script makes that
# copy; .github/workflows/dbt-mirror.yml and the release workflow run it.
#
#   scripts/mirror_dbt.sh layout DIR [REF]   DIR becomes the package as published: the files git tracks under
#                                            dbt/, LICENSE, and the README with links that leave dbt/ pointing at
#                                            this repository on GitHub at REF (default main). DIR's .git is kept;
#                                            everything else in it is replaced.
#   scripts/mirror_dbt.sh publish DIR        In DIR, a clone of dbt-duckwake laid out as above: commit what changed
#                                            and push it to main. Nothing to commit, nothing pushed.
#   scripts/mirror_dbt.sh publish DIR TAG    Tag the laid-out tree TAG and push the tag. When main already holds
#                                            that tree the tag points at main's head; otherwise (main moved on
#                                            since the release commit) at a commit of its own whose parent is
#                                            main's head, so main never goes back. A TAG the remote already has
#                                            for another tree is refused.
#
# SOURCE_SHA names the commit of this repository being mirrored (default: HEAD) in the commit messages.
set -euo pipefail
cd "$(dirname "$0")/.."
GITHUB=https://github.com/duckwake/duckwake

die() { echo "mirror_dbt: $*" >&2; exit 1; }

layout() {
  local dir=$1 ref=${2:-main}
  [[ -f dbt/dbt_project.yml ]] || die "no dbt/dbt_project.yml"
  mkdir -p "$dir"
  find "$dir" -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
  # Only what git tracks: no target/, dbt_packages/ or logs/ from a local run.
  (cd dbt && git ls-files -z . | tar --null -T - -cf -) | tar -xf - -C "$dir"
  [[ -f $dir/dbt_project.yml ]] || die "dbt/ has no tracked files (is this a git checkout?)"
  cp LICENSE "$dir/LICENSE"
  # The README: a note on where the package is written, and every link out of dbt/ made absolute.
  awk -v gh="$GITHUB" -v ref="$ref" '
    { gsub(/\]\(\.\.\//, "](" gh "/blob/" ref "/") }
    { print }
    /^# / && !noted {
      print ""
      print "> This repository is published from [`dbt/` in duckwake/duckwake](" gh "/tree/" ref "/dbt), where the package"
      print "> is written and tested against the engine. Send issues and pull requests there."
      noted = 1
    }' dbt/README.md >"$dir/README.md"
  if grep -n '](\.\./' "$dir/README.md"; then die "README.md still has links out of dbt/"; fi
  mkdir -p "$dir/.github"
  cat >"$dir/.github/pull_request_template.md" <<EOF
This repository is a copy of [\`dbt/\` in duckwake/duckwake]($GITHUB/tree/main/dbt), replaced on every change
there. Please open the pull request against duckwake/duckwake instead; a change made here is overwritten.
EOF
}

publish() {
  local dir=$1 tag=${2:-} src msg tree head=""
  src=${SOURCE_SHA:-$(git rev-parse HEAD)}
  msg="Mirror duckwake/duckwake@${src:0:12}

From $GITHUB/commit/$src"
  cd "$dir"
  # A clone whose remote HEAD names another branch checks nothing out: start from main if the remote has it.
  if ! git rev-parse -q --verify HEAD >/dev/null && git rev-parse -q --verify refs/remotes/origin/main >/dev/null; then
    git update-ref refs/heads/main refs/remotes/origin/main
    git symbolic-ref HEAD refs/heads/main
    git read-tree HEAD   # the index only; the laid-out files stay
  fi
  git rev-parse -q --verify HEAD >/dev/null && head=$(git rev-parse HEAD)
  git add -A
  if [[ -z $tag ]]; then
    if [[ -n $head ]] && git diff --cached --quiet; then echo "mirror_dbt: main is up to date"; return; fi
    git commit -q -m "$msg"
    git push -q origin HEAD:refs/heads/main
    echo "mirror_dbt: pushed $(git rev-parse --short HEAD) to main"
    return
  fi
  tree=$(git write-tree)
  local commit
  if [[ -n $head ]] && [[ $(git rev-parse "$head^{tree}") == "$tree" ]]; then
    commit=$head
  else
    commit=$(git commit-tree "$tree" ${head:+-p "$head"} -m "$msg ($tag)")
  fi
  local remote
  remote=$(git ls-remote origin "refs/tags/$tag^{}" | cut -f1)
  [[ -n $remote ]] || remote=$(git ls-remote origin "refs/tags/$tag" | cut -f1)
  if [[ -n $remote ]]; then
    git fetch -q --depth 1 origin "refs/tags/$tag:refs/tags/$tag"
    [[ $(git rev-parse "refs/tags/$tag^{tree}") == "$tree" ]] || die "$tag already exists with other content"
    echo "mirror_dbt: $tag is already published"
    return
  fi
  git tag -a "$tag" "$commit" -m "dbt-duckwake $tag, from duckwake/duckwake@${src:0:12}"
  git push -q origin "refs/tags/$tag"
  echo "mirror_dbt: pushed $tag at $(git rev-parse --short "$commit")"
}

case ${1:-} in
  layout) shift; [[ $# -ge 1 ]] || die "usage: layout DIR [REF]"; layout "$@" ;;
  publish) shift; [[ $# -ge 1 ]] || die "usage: publish DIR [TAG]"; publish "$@" ;;
  *) die "usage: $0 layout DIR [REF] | publish DIR [TAG]" ;;
esac
