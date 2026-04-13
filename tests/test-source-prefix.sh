#!/bin/bash


set -e

source tests/utils.sh

use_gnu_tar_if_available

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/test-tardiff-fuzzy-abs-XXXXXX")
TREE="$TEST_DIR/tree"

cleanup () {
	rm -rf "$TEST_DIR"
}
trap cleanup EXIT

create_v1 () {
	rm -rf "$TREE"
	mkdir -p "$TREE/blobs/sha256" "$TREE/config" "$TREE/data"
	printf '%s' 'blob-content'   > "$TREE/blobs/sha256/abc123"
	printf '%s' 'config-v1'     > "$TREE/config/app.conf"
	printf '%s' 'data-v1'        > "$TREE/data/file.txt"
}

create_v2 () {
	rm -rf "$TREE"
	mkdir -p "$TREE/blobs/sha256" "$TREE/config" "$TREE/data"
	printf '%s' 'blob-content-modified' > "$TREE/blobs/sha256/abc123"
	printf '%s' 'config-v2'              > "$TREE/config/app.conf"
	printf '%s' 'data-v2'                > "$TREE/data/file.txt"
}

create_tar () {
	local file=$1
	local root=$2
	tar cf "$file" -C "$root" blobs config data
    compress_tar "$file"
}

unpack_old_tree () {
	local dest=$1
	rm -rf "$dest"
	mkdir -p "$dest"
	tar xf "$TEST_DIR/old.tar" -C "$dest"
}

create_v1
create_tar "$TEST_DIR/old.tar" "$TREE"

create_v2
create_tar "$TEST_DIR/new.tar" "$TREE"

unpack_old_tree "$TEST_DIR/unpack1"

echo "Generating tardiff (--source-prefix=blobs/)"
./tar-diff --source-prefix=blobs/ "$TEST_DIR/old.tar.gz" "$TEST_DIR/new.tar.bz2" "$TEST_DIR/delta1.tardiff"

echo "Applying tardiff (single prefix)"
./tar-patch "$TEST_DIR/delta1.tardiff" "$TEST_DIR/unpack1" "$TEST_DIR/out1.tar"

echo "Verifying (single prefix)"
cmp "$TEST_DIR/out1.tar" "$TEST_DIR/new.tar"

unpack_old_tree "$TEST_DIR/unpack2"

echo "Generating tardiff (--source-prefix=blobs/ --source-prefix=config/)"
./tar-diff --source-prefix=blobs/ --source-prefix=config/ "$TEST_DIR/old.tar.gz" "$TEST_DIR/new.tar.bz2" "$TEST_DIR/delta2.tardiff"

echo "Applying tardiff (dual prefix)"
./tar-patch "$TEST_DIR/delta2.tardiff" "$TEST_DIR/unpack2" "$TEST_DIR/out2.tar"

echo "Verifying (dual prefix)"
cmp "$TEST_DIR/out2.tar" "$TEST_DIR/new.tar"

echo OK source-prefix
