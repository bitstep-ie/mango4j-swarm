#!/bin/sh
set -eu

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "release.sh requires a clean working tree." >&2
  exit 1
fi

snapshot_version=$(scripts/bump-version.py --pom pom.xml --current)
case "$snapshot_version" in
  *-SNAPSHOT) ;;
  *)
    echo "Project version is not a snapshot: ${snapshot_version}" >&2
    exit 1
    ;;
esac
version=${snapshot_version%-SNAPSHOT}
tag="v${version}"
branch="release/${tag}"

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "Tag ${tag} already exists." >&2
  exit 1
fi

if git rev-parse -q --verify "refs/heads/${branch}" >/dev/null; then
  git switch "$branch"
else
  git switch -c "$branch"
fi

original_pom=$(mktemp)
cp pom.xml "$original_pom"
restore_pom() {
  if [ -f "$original_pom" ]; then
    cp "$original_pom" pom.xml
    rm -f "$original_pom"
  fi
}
trap restore_pom EXIT INT HUP TERM

version=$(scripts/bump-version.py --pom pom.xml --release-from-snapshot)

mvn --batch-mode --no-transfer-progress -P hammer-time

non_version_changes=$(git diff --name-only | grep -vx 'pom.xml' || true)
if [ -n "$non_version_changes" ]; then
  echo "Release gate modified files other than pom.xml:" >&2
  echo "$non_version_changes" >&2
  exit 1
fi

git add pom.xml
git commit -m "chore: release ${version}"
git tag -a "${tag}" -m "Release ${tag}"

rm -f "$original_pom"
trap - EXIT INT HUP TERM

echo "Created release branch ${branch}, release commit, and tag ${tag}."
