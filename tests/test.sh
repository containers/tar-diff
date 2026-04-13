#!/bin/bash

set -e

source tests/utils.sh

use_gnu_tar_if_available

echo "Checking -version"
td_line=$(./tar-diff -version 2>/dev/null | head -n1)
tp_line=$(./tar-patch -version 2>/dev/null | head -n1)
td_ver=${td_line##* }
tp_ver=${tp_line##* }
if [[ -z "$td_ver" || ! "$td_ver" =~ ^v[0-9] ]]; then
	echo "unexpected ./tar-diff -version (first line: ${td_line})" >&2
	exit 1
fi
if [[ -z "$tp_ver" || ! "$tp_ver" =~ ^v[0-9] ]]; then
	echo "unexpected ./tar-patch -version (first line: ${tp_line})" >&2
	exit 1
fi


TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/test-tardiff-XXXXXX")

cleanup () {
    rm -rf $TEST_DIR
}
trap cleanup EXIT

# --- Basic single-layer test ---

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

# --- Multi-layer test (simulating OCI image layers with whiteouts) ---

create_multi_orig1 () {
    DIR=$1
    mkdir -p $DIR/data/subdir
    echo "base-file1" > $DIR/data/file1.txt
    echo "base-file2" > $DIR/data/file2.txt
    dd if=/dev/urandom of=$DIR/data/deleted.txt bs=1024 count=64 2>/dev/null
    dd if=/dev/urandom of=$DIR/data/subdir/a.txt bs=1024 count=64 2>/dev/null
    dd if=/dev/urandom of=$DIR/data/subdir/b.txt bs=1024 count=64 2>/dev/null
}

create_multi_orig2 () {
    DIR=$1
    mkdir -p $DIR/data/subdir
    echo "layer2-file2-override" > $DIR/data/file2.txt
    echo "layer2-file3" > $DIR/data/file3.txt
    touch $DIR/data/.wh.deleted.txt
    touch $DIR/data/subdir/.wh..wh..opq
    echo "new-subdir-file" > $DIR/data/subdir/c.txt
}

create_multi_modified () {
    DIR=$1
    ORIG1_DIR=$2
    mkdir -p $DIR/data/subdir
    echo "base-file1" > $DIR/data/file1.txt
    echo "layer2-file2-override" > $DIR/data/file2.txt
    echo "layer3-file3-modified" > $DIR/data/file3.txt
    echo "new-subdir-file" > $DIR/data/subdir/c.txt
    cp $ORIG1_DIR/data/deleted.txt $DIR/data/deleted.txt
    echo "extra" >> $DIR/data/deleted.txt
    cp $ORIG1_DIR/data/subdir/a.txt $DIR/data/subdir/a.txt
    echo "extra" >> $DIR/data/subdir/a.txt
}

echo "Testing multi-layer tar diff (simulating OCI image layers)"

create_multi_orig1 $TEST_DIR/multi_orig1
create_tar $TEST_DIR/multi_orig1.tar $TEST_DIR/multi_orig1

create_multi_orig2 $TEST_DIR/multi_orig2
create_tar $TEST_DIR/multi_orig2.tar $TEST_DIR/multi_orig2

create_multi_modified $TEST_DIR/multi_modified $TEST_DIR/multi_orig1
create_tar $TEST_DIR/multi_modified.tar $TEST_DIR/multi_modified

# Build combined directories for patching
mkdir -p $TEST_DIR/combined-full
tar xf $TEST_DIR/multi_orig1.tar -C $TEST_DIR/combined-full
tar xf $TEST_DIR/multi_orig2.tar -C $TEST_DIR/combined-full

mkdir -p $TEST_DIR/combined-merged
tar xf $TEST_DIR/multi_orig1.tar -C $TEST_DIR/combined-merged
rm -f $TEST_DIR/combined-merged/data/deleted.txt
rm -rf $TEST_DIR/combined-merged/data/subdir
mkdir -p $TEST_DIR/combined-merged/data/subdir
tar xf $TEST_DIR/multi_orig2.tar -C $TEST_DIR/combined-merged --exclude='*/.wh.*'

# Test 1: without --apply-whiteouts
echo "Test 1: multi-layer diff without --apply-whiteouts"
./tar-diff $TEST_DIR/multi_orig1.tar $TEST_DIR/multi_orig2.tar $TEST_DIR/multi_modified.tar $TEST_DIR/no-whiteout.tardiff

echo "Applying tardiff (without whiteouts, full combined directory)"
./tar-patch $TEST_DIR/no-whiteout.tardiff $TEST_DIR/combined-full $TEST_DIR/reconstructed1.tar

echo "Verifying reconstruction"
cmp $TEST_DIR/reconstructed1.tar $TEST_DIR/multi_modified.tar

echo "Verifying that applying without --apply-whiteouts fails on merged directory"
if ./tar-patch $TEST_DIR/no-whiteout.tardiff $TEST_DIR/combined-merged $TEST_DIR/should-fail.tar 2>/dev/null; then
    if cmp -s $TEST_DIR/should-fail.tar $TEST_DIR/multi_modified.tar; then
        echo "  (diff did not reference whited-out files, reconstruction still correct)"
    else
        echo "  FAIL: reconstruction was incorrect"
        exit 1
    fi
else
    echo "  OK: tar-patch correctly failed (missing whited-out source files)"
fi

# Test 2: with --apply-whiteouts
echo "Test 2: multi-layer diff with --apply-whiteouts"
./tar-diff --apply-whiteouts $TEST_DIR/multi_orig1.tar $TEST_DIR/multi_orig2.tar $TEST_DIR/multi_modified.tar $TEST_DIR/with-whiteout.tardiff

echo "Applying tardiff (with whiteouts, merged directory)"
./tar-patch $TEST_DIR/with-whiteout.tardiff $TEST_DIR/combined-merged $TEST_DIR/reconstructed2.tar

echo "Verifying reconstruction"
cmp $TEST_DIR/reconstructed2.tar $TEST_DIR/multi_modified.tar

echo "Applying tardiff (with whiteouts, full combined directory)"
./tar-patch $TEST_DIR/with-whiteout.tardiff $TEST_DIR/combined-full $TEST_DIR/reconstructed3.tar

echo "Verifying reconstruction"
cmp $TEST_DIR/reconstructed3.tar $TEST_DIR/multi_modified.tar

echo "All tests OK"
