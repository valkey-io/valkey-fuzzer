#!/usr/bin/env bash
# Build the bug zoo's Valkey binaries.
#
#   zoo/build.sh OUT [CASE ...]
#
# Clones https://github.com/valkey-io/valkey into OUT/source (once), checks out
# the commit in zoo/valkey-commit, and builds it unpatched into OUT/clean. Each
# case with a patch.diff (all of them, or only the CASEs named) is then built
# into OUT/<case> from a copy of OUT/clean with its patch applied, so only the
# patched files recompile. The binaries are OUT/clean/src/valkey-server and
# OUT/<case>/src/valkey-server. Works on Linux and macOS; set JOBS to override
# the parallelism and MAKE_ARGS to pass extra make variables (BUILD_TLS=yes).
set -euo pipefail

ZOO=$(cd "$(dirname "$0")" && pwd)
OUT=${1:?usage: zoo/build.sh OUT [CASE ...]}
shift
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
COMMIT=$(tr -d '[:space:]' < "$ZOO/valkey-commit")
JOBS=${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}
read -r -a EXTRA <<< "${MAKE_ARGS:-}"

build() { # build DIR LOG
    make -C "$1" -j"$JOBS" ${EXTRA[@]+"${EXTRA[@]}"} valkey-server > "$2" 2>&1 \
        || { echo "build failed in $1; see $2" >&2; tail -20 "$2" >&2; exit 1; }
}

if [ ! -d "$OUT/source/.git" ]; then
    git clone --quiet https://github.com/valkey-io/valkey "$OUT/source"
fi
if ! git -C "$OUT/source" cat-file -e "$COMMIT^{commit}" 2>/dev/null; then
    git -C "$OUT/source" fetch --quiet origin
fi

if [ "$(git -C "$OUT/clean" rev-parse HEAD 2>/dev/null)" != "$COMMIT" ] \
    || ! git -C "$OUT/clean" diff --quiet || [ ! -x "$OUT/clean/src/valkey-server" ]; then
    rm -rf "$OUT/clean"
    git clone --quiet --no-checkout "$OUT/source" "$OUT/clean"
    git -C "$OUT/clean" checkout --quiet --detach "$COMMIT"
    echo "building clean $COMMIT"
    build "$OUT/clean" "$OUT/clean.build.log"
fi
echo "clean: $OUT/clean/src/valkey-server"

if [ $# -eq 0 ]; then
    set -- $(cd "$ZOO" && ls -d */ | tr -d /)
fi
for case_id in "$@"; do
    [ -f "$ZOO/$case_id/case.yaml" ] || { echo "no such case: $case_id" >&2; exit 1; }
    [ -f "$ZOO/$case_id/patch.diff" ] || continue
    rm -rf "${OUT:?}/$case_id"
    # cp -p keeps the clean build's timestamps, so make rebuilds only what the patch touches.
    cp -Rp "$OUT/clean" "$OUT/$case_id"
    git -C "$OUT/$case_id" apply --whitespace=nowarn "$ZOO/$case_id/patch.diff"
    echo "building $case_id"
    build "$OUT/$case_id" "$OUT/$case_id.build.log"
    echo "$case_id: $OUT/$case_id/src/valkey-server"
done
