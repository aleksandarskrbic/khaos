#!/usr/bin/env bash
# Cuts a release: drafts a CHANGELOG.md entry from conventional commits since the last
# tag, then (after confirmation) commits, pushes main, tags, and pushes the tag -- which
# triggers .github/workflows/release.yml (GoReleaser).
#
# Usage: scripts/release.sh <version> [--yes]
#   version   semver without the leading "v", e.g. 0.10.0
#   --yes     skip the confirmation prompt (still prints the draft first)
set -euo pipefail

VERSION="${1:-}"
YES=0
for arg in "$@"; do
    [ "$arg" = "--yes" ] && YES=1
done

if [ -z "$VERSION" ] || [[ "$VERSION" == --* ]]; then
    echo "usage: scripts/release.sh <version> [--yes]" >&2
    echo "example: scripts/release.sh 0.10.0" >&2
    exit 1
fi
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: version must be semver without a leading 'v' (got: $VERSION)" >&2
    exit 1
fi
TAG="v$VERSION"

echo "==> preflight"
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "main" ]; then
    echo "error: must be on main (currently on $BRANCH)" >&2
    exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
    echo "error: working tree is not clean" >&2
    exit 1
fi
git fetch --quiet --tags
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "error: tag $TAG already exists" >&2
    exit 1
fi
git pull --quiet --ff-only origin main

LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LAST_TAG" ]; then
    echo "==> commits since $LAST_TAG"
    RANGE="$LAST_TAG..HEAD"
else
    echo "==> no previous tag found, using full history"
    RANGE="HEAD"
fi

ADDED=()
FIXED=()
CHANGED=()
SKIPPED=0

while IFS= read -r subject; do
    [ -z "$subject" ] && continue
    if [[ "$subject" =~ ^(feat|fix|perf|refactor|docs)(\(([a-zA-Z0-9_/-]+)\))?!?:[[:space:]]*(.+)$ ]]; then
        type="${BASH_REMATCH[1]}"
        scope="${BASH_REMATCH[3]}"
        desc="${BASH_REMATCH[4]}"
        desc="$(tr '[:lower:]' '[:upper:]' <<<"${desc:0:1}")${desc:1}"
        if [ -n "$scope" ]; then
            bullet="- **${scope}:** ${desc}"
        else
            bullet="- ${desc}"
        fi
        case "$type" in
            feat) ADDED+=("$bullet") ;;
            fix) FIXED+=("$bullet") ;;
            perf | refactor | docs) CHANGED+=("$bullet") ;;
        esac
    else
        SKIPPED=$((SKIPPED + 1))
    fi
done < <(git log "$RANGE" --no-merges --pretty=format:%s)

if [ "$SKIPPED" -gt 0 ]; then
    echo "(skipped $SKIPPED non-conventional or chore/ci/test/build/style commit(s))"
fi
if [ ${#ADDED[@]} -eq 0 ] && [ ${#FIXED[@]} -eq 0 ] && [ ${#CHANGED[@]} -eq 0 ]; then
    echo "error: no feat/fix/perf/refactor/docs commits since $LAST_TAG -- write the CHANGELOG.md entry by hand" >&2
    exit 1
fi

echo "==> drafting CHANGELOG.md entry"
DATE=$(date +%Y-%m-%d)
{
    echo "## [$VERSION] - $DATE"
    echo
    if [ ${#ADDED[@]} -gt 0 ]; then
        echo "### Added"
        printf '%s\n' "${ADDED[@]}"
        echo
    fi
    if [ ${#FIXED[@]} -gt 0 ]; then
        echo "### Fixed"
        printf '%s\n' "${FIXED[@]}"
        echo
    fi
    if [ ${#CHANGED[@]} -gt 0 ]; then
        echo "### Changed"
        printf '%s\n' "${CHANGED[@]}"
        echo
    fi
} >/tmp/khaos-changelog-entry.md

echo
cat /tmp/khaos-changelog-entry.md
echo "---"

if [ "$YES" -ne 1 ]; then
    read -r -p "Insert this into CHANGELOG.md, commit, tag $TAG, and push? [y/N] " REPLY
    if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
        echo "aborted, nothing changed"
        rm -f /tmp/khaos-changelog-entry.md
        exit 1
    fi
fi

awk -v entryfile=/tmp/khaos-changelog-entry.md '
    !inserted && /^## \[/ {
        while ((getline line < entryfile) > 0) print line
        inserted = 1
    }
    { print }
' CHANGELOG.md >CHANGELOG.md.tmp
mv CHANGELOG.md.tmp CHANGELOG.md
rm -f /tmp/khaos-changelog-entry.md

echo "==> commit, tag, push"
git add CHANGELOG.md
git commit -m "docs: add $VERSION changelog entry"
git push origin main
git tag -a "$TAG" -m "$TAG"
git push origin "$TAG"

echo "==> done. Release workflow triggered: https://github.com/aleksandarskrbic/khaos/actions/workflows/release.yml"
