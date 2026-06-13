#!/bin/sh
set -eu

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "release.sh requires a clean working tree." >&2
  exit 1
fi

version=$(scripts/bump-patch-version.py --pom pom.xml)
tag="v${version}"

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "Tag ${tag} already exists." >&2
  exit 1
fi

mvn --batch-mode --no-transfer-progress -P hammer-time

git add pom.xml
git commit -m "chore: bump version to ${version}"
git tag -a "${tag}" -m "Release ${tag}"

echo "Created release commit and tag ${tag}."
