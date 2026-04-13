#!/bin/bash

set -e

source tests/utils.sh

use_gnu_tar_if_available

TEST_DIR=$(mktemp -d /tmp/test-tardiff-fuzzy-abs-XXXXXX)

mkdir -p "$TEST_DIR/orig/data"
printf 'aaaaaaaaaa' >"$TEST_DIR/orig/data/foo.a"
head -c 50 /dev/zero | tr '\0' 'b' >"$TEST_DIR/orig/data/foo.b"

mkdir -p "$TEST_DIR/modified/data"
printf 'cccccccccccccccccccc' >"$TEST_DIR/modified/data/foo.c"

create_tar "$TEST_DIR/old.tar" "$TEST_DIR/orig"
create_tar "$TEST_DIR/new.tar" "$TEST_DIR/modified"

echo "Generating tardiff (fuzzy rename: foo.a + foo.b -> foo.c)"
./tar-diff "$TEST_DIR/old.tar.gz" "$TEST_DIR/new.tar.bz2" "$TEST_DIR/changes.tardiff"

echo "Applying tardiff"
./tar-patch "$TEST_DIR/changes.tardiff" "$TEST_DIR/orig" "$TEST_DIR/reconstructed.tar"

echo "Verifying reconstruction"
cmp "$TEST_DIR/reconstructed.tar" "$TEST_DIR/new.tar"

echo OK fuzzy-abs

cleanup () {
	rm -rf "$TEST_DIR"
}
trap cleanup EXIT
