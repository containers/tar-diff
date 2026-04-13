#!/bin/bash

set -e

source tests/utils.sh

use_gnu_tar_if_available

WORKDIR=$(mktemp -d /tmp/test-tardiff-delta-paths-XXXXXX)

# Part A: rollsum + copyRest
TEST_DIR="$WORKDIR/rollsum"
BIG_BYTES=$((3 * 1024 * 1024))
OTHER_BYTES=$((2 * 1024 * 1024))

mkdir -p "$TEST_DIR/orig/data" "$TEST_DIR/modified/data"

head -c "$BIG_BYTES" /dev/zero >"$TEST_DIR/orig/data/big.bin"
cp -a "$TEST_DIR/orig/data/big.bin" "$TEST_DIR/modified/data/big.bin"
printf 'ROLLSUM_INTEGRATION' | dd of="$TEST_DIR/modified/data/big.bin" bs=1 seek=1500000 conv=notrunc status=none 2>/dev/null || \
	printf 'ROLLSUM_INTEGRATION' | dd of="$TEST_DIR/modified/data/big.bin" bs=1 seek=1500000 conv=notrunc 2>/dev/null

head -c "$OTHER_BYTES" /dev/zero >"$TEST_DIR/orig/data/other.bin"
head -c "$OTHER_BYTES" /dev/zero | LC_ALL=C tr '\000' '\377' >"$TEST_DIR/modified/data/other.bin"

create_tar "$TEST_DIR/old.tar" "$TEST_DIR/orig"
create_tar "$TEST_DIR/new.tar" "$TEST_DIR/modified"

echo "Generating tardiff A (-max-bsdiff-size 1: rollsum + copyRest)"
./tar-diff -max-bsdiff-size 1 "$TEST_DIR/old.tar.gz" "$TEST_DIR/new.tar.bz2" "$TEST_DIR/changes.tardiff"

echo "Applying tardiff A"
./tar-patch "$TEST_DIR/changes.tardiff" "$TEST_DIR/orig" "$TEST_DIR/reconstructed.tar"

echo "Verifying A"
cmp "$TEST_DIR/reconstructed.tar" "$TEST_DIR/new.tar"

echo OK rollsum-and-copyRest

# Part B: bsdiff + qsufsort/split (no low max-bsdiff-size)
BS="$WORKDIR/bsdiff"
BSDIFF_BYTES=$((512 * 1024))

mkdir -p "$BS/orig/data" "$BS/modified/data"
if ! head -c "$BSDIFF_BYTES" /dev/urandom >"$BS/orig/data/large.bin" 2>/dev/null; then
	head -c "$BSDIFF_BYTES" /dev/zero >"$BS/orig/data/large.bin"
fi
cp -a "$BS/orig/data/large.bin" "$BS/modified/data/large.bin"
printf 'PATCH_BSDIFF_INTEGRATION_0123456789' | dd of="$BS/modified/data/large.bin" bs=1 seek=200000 conv=notrunc status=none 2>/dev/null || \
	printf 'PATCH_BSDIFF_INTEGRATION_0123456789' | dd of="$BS/modified/data/large.bin" bs=1 seek=200000 conv=notrunc 2>/dev/null

create_tar "$BS/old.tar" "$BS/orig"
create_tar "$BS/new.tar" "$BS/modified"

echo "Generating tardiff B (default max-bsdiff: medium file → bsdiff / qsufsort)"
./tar-diff "$BS/old.tar.gz" "$BS/new.tar.bz2" "$BS/changes.tardiff"

echo "Applying tardiff B"
./tar-patch "$BS/changes.tardiff" "$BS/orig" "$BS/reconstructed.tar"

echo "Verifying B"
cmp "$BS/reconstructed.tar" "$BS/new.tar"

echo OK bsdiff-large-old

cleanup () {
	rm -rf "$WORKDIR"
}
trap cleanup EXIT
