#!/bin/bash

set -e

source tests/utils.sh

use_gnu_tar_if_available

# Usage: tests/test-multi-old.sh [N]
#        N defaults to MULTI_OLD_N or 3. Example: MULTI_OLD_N=6 tests/test-multi-old.sh
N="${1:-${MULTI_OLD_N:-3}}"

if [[ "$N" =~ ^[0-9]+$ ]] && ((N >= 2)); then
	:
else
	echo "usage: $0 [N]  (integer N >= 2, or set MULTI_OLD_N)" >&2
	exit 1
fi

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/test-tardiff-multi-old-XXXXXX")
PEEL_COUNT=$((N - 1))

cleanup () {
	rm -rf "$TEST_DIR"
}
trap cleanup EXIT

# Relative paths under data/ to peel into separate old layers (order matches older N=2 case: bar first).
declare -a order=( "dir1/bar.txt" "dir1/foo.txt" "dir1/move.txt" )
declare -a PEEL=()

create_orig "$TEST_DIR/orig"

for ((i = 0; i < PEEL_COUNT; i++)); do
	if ((i < ${#order[@]})); then
		PEEL+=("${order[i]}")
	else
		k=$((i - ${#order[@]} + 1))
		mkdir -p "$TEST_DIR/orig/data/dir1"
		echo "multi-pad-$k" >"$TEST_DIR/orig/data/dir1/_multi_$k.txt"
		PEEL+=("dir1/_multi_$k.txt")
	fi
done

create_tar "$TEST_DIR/orig.tar" "$TEST_DIR/orig"
modify_orig "$TEST_DIR/modified" "$TEST_DIR/orig.tar"
create_tar "$TEST_DIR/modified.tar" "$TEST_DIR/modified"

# old1: full tree minus every peeled file
cp -dR --no-preserve=context "$TEST_DIR/orig" "$TEST_DIR/mo-base" 2>/dev/null \
	|| cp -dR "$TEST_DIR/orig" "$TEST_DIR/mo-base"
for rel in "${PEEL[@]}"; do
	rm -f "$TEST_DIR/mo-base/data/$rel"
done

# old2..oldN: one peeled file each
for ((i = 0; i < PEEL_COUNT; i++)); do
	rel="${PEEL[i]}"
	layer=$((i + 2))
	sdir="$TEST_DIR/mo-slice-$layer"
	mkdir -p "$sdir/data/$(dirname "$rel")"
	cp "$TEST_DIR/orig/data/$rel" "$sdir/data/$rel"
	create_tar "$TEST_DIR/mo-old${layer}.tar" "$sdir"
done

create_tar "$TEST_DIR/mo-old1.tar" "$TEST_DIR/mo-base"

declare -a DIFF_ARGS=()
for ((i = 1; i <= N; i++)); do
	DIFF_ARGS+=("$TEST_DIR/mo-old${i}.tar.gz")
done
DIFF_ARGS+=("$TEST_DIR/modified.tar.bz2" "$TEST_DIR/mo-changes.tardiff")

echo "Generating tardiff (N=$N multi-old)"
./tar-diff "${DIFF_ARGS[@]}"

echo "Applying tardiff (N=$N)"
./tar-patch "$TEST_DIR/mo-changes.tardiff" "$TEST_DIR/orig" "$TEST_DIR/mo-reconstructed.tar"

echo "Verifying reconstruction (N=$N)"
cmp "$TEST_DIR/mo-reconstructed.tar" "$TEST_DIR/modified.tar"

echo "OK multi-old (N=$N)"
