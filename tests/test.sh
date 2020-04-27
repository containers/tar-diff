#!/bin/bash

set -e

TEST_DIR=$(mktemp -d /tmp/test-tardiff-XXXXXX)

create_orig () {
    DIR=$1

    mkdir -p $DIR
    pushd $DIR &> /dev/null

    mkdir data
    mkdir data/dir1
    mkdir data/dir2
    echo foo > data/dir1/foo.txt
    echo bar > data/dir1/bar.txt
    echo movedata > data/dir1/move.txt
    ln -s not-exist data/broken
    ln -s foo.txt data/dir1/symlink
    ln data/dir1/foo.txt data/dir1/hardlink

    echo "PART1" > data/sparse
    dd of=data/sparse if=/dev/null bs=1024k seek=1 count=1 &> /dev/null
    echo "PART2" >> data/sparse

    popd &> /dev/null
}

modify_orig () {
    DIR=$1
    SRC=$2

    mkdir -p $DIR
    # Extract old data
    tar xf  $SRC -C $DIR
    pushd $DIR &> /dev/null

    # Modify it
    echo newdata > data/newfile
    mv data/dir1/move.txt data/dir2/move.txt

    echo bar >> data/dir1/bar.txt
    mv data/dir1/bar.txt data/dir1/bar.TXT # Rename we should pick up

    popd &> /dev/null
}

compress_tar () {
    FILE=$1
    gzip --keep $FILE
    bzip2 --keep $FILE
}

create_tar () {
    FILE=$1
    DIR=$2
    tar cf $FILE --sparse -C $DIR data
    compress_tar $FILE
}

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
