#!/bin/bash

set -e

source tests/utils.sh

use_gnu_tar_if_available

# Smoke-test for -version 
echo "Checking -version"
read -r _td_name td_ver < <(./tar-diff -version)
read -r _tp_name tp_ver < <(./tar-patch -version)
if [[ -z "$td_ver" || "$td_ver" != v* ]]; then
	echo "unexpected ./tar-diff -version output (${_td_name} ${td_ver})" >&2
	exit 1
fi
if [[ -z "$tp_ver" || "$tp_ver" != v* ]]; then
	echo "unexpected ./tar-patch -version output (${_tp_name} ${tp_ver})" >&2
	exit 1
fi

TEST_DIR=$(mktemp -d /tmp/test-tardiff-XXXXXX)


create_orig $TEST_DIR/orig
create_tar $TEST_DIR/orig.tar $TEST_DIR/orig

modify_orig $TEST_DIR/modified $TEST_DIR/orig.tar
create_tar $TEST_DIR/modified.tar $TEST_DIR/modified

echo Generating tardiff
./tar-diff $TEST_DIR/orig.tar.gz $TEST_DIR/modified.tar.bz2 $TEST_DIR/changes.tardiff

echo Applying tardiff
./tar-patch $TEST_DIR/changes.tardiff $TEST_DIR/orig $TEST_DIR/reconstructed.tar

echo Verifying reconstruction
cmp $TEST_DIR/reconstructed.tar $TEST_DIR/modified.tar

echo OK

cleanup () {
    rm -rf $TEST_DIR
}
trap cleanup EXIT
