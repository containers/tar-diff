#!/bin/bash

# Integration: expected failures for tar-diff and tar-patch CLIs (cover binaries).

set -e

source tests/utils.sh

use_gnu_tar_if_available

TEST_DIR=$(mktemp -d /tmp/test-tardiff-cli-err-XXXXXX)

expect_fail () {
	local desc=$1
	shift
	set +e
	"$@" &>/dev/null
	local code=$?
	set -e
	if [[ "$code" -eq 0 ]]; then
		echo "expected failure for: $desc" >&2
		exit 1
	fi
}

mkdir -p "$TEST_DIR/orig/data"

# tar-diff: CLI / I/O errors
expect_fail "tar-diff too few args" ./tar-diff
expect_fail "tar-diff two args only" ./tar-diff one two

dd if=/dev/urandom of="$TEST_DIR/junk.gz" bs=200 count=1 status=none 2>/dev/null || \
	dd if=/dev/urandom of="$TEST_DIR/junk.gz" bs=200 count=1 2>/dev/null
expect_fail "tar-diff missing old archive" ./tar-diff "$TEST_DIR/does-not-exist.gz" "$TEST_DIR/junk.gz" "$TEST_DIR/delta-x.tardiff"

mkdir -p "$TEST_DIR/td-a/data" "$TEST_DIR/td-b/data"
echo x >"$TEST_DIR/td-a/data/f.txt"
echo y >"$TEST_DIR/td-b/data/f.txt"
create_tar "$TEST_DIR/td-old.tar" "$TEST_DIR/td-a"
create_tar "$TEST_DIR/td-new.tar" "$TEST_DIR/td-b"
expect_fail "tar-diff missing new archive" ./tar-diff "$TEST_DIR/td-old.tar.gz" "$TEST_DIR/missing-new.tar.gz" "$TEST_DIR/td-out.tardiff"

expect_fail "tar-diff corrupt archive inputs" ./tar-diff "$TEST_DIR/junk.gz" "$TEST_DIR/junk.gz" "$TEST_DIR/td-badio.tardiff"

# tar-patch: invalid / corrupt delta
printf 'not-a-tardiff' >"$TEST_DIR/bad-magic.tardiff"
expect_fail "tar-patch bad magic" ./tar-patch "$TEST_DIR/bad-magic.tardiff" "$TEST_DIR/orig" "$TEST_DIR/out1.tar"

printf '\x74\x61\x72\x64\x66\x31\x0a\x00' >"$TEST_DIR/bad-zstd.tardiff"
printf '\x00\x01\x02\x03' >>"$TEST_DIR/bad-zstd.tardiff"
expect_fail "tar-patch invalid zstd" ./tar-patch "$TEST_DIR/bad-zstd.tardiff" "$TEST_DIR/orig" "$TEST_DIR/out2.tar"

expect_fail "tar-patch missing base dir" ./tar-patch "$TEST_DIR/bad-magic.tardiff" "$TEST_DIR/does-not-exist" "$TEST_DIR/out3.tar"

# tar-patch: valid delta, missing source file
mkdir -p "$TEST_DIR/solo/data" "$TEST_DIR/solom/data"
echo hello >"$TEST_DIR/solo/data/only.txt"
echo 'hello!' >"$TEST_DIR/solom/data/only.txt"
create_tar "$TEST_DIR/solo-old.tar" "$TEST_DIR/solo"
create_tar "$TEST_DIR/solo-new.tar" "$TEST_DIR/solom"
./tar-diff "$TEST_DIR/solo-old.tar.gz" "$TEST_DIR/solo-new.tar.bz2" "$TEST_DIR/solo.tardiff"
rm -f "$TEST_DIR/solo/data/only.txt"
expect_fail "tar-patch missing source member" ./tar-patch "$TEST_DIR/solo.tardiff" "$TEST_DIR/solo" "$TEST_DIR/solo-out.tar"

# tar-patch: stdout destination (happy path)
mkdir -p "$TEST_DIR/st/data" "$TEST_DIR/stm/data"
echo hello >"$TEST_DIR/st/data/only.txt"
echo 'hello!' >"$TEST_DIR/stm/data/only.txt"
create_tar "$TEST_DIR/st-old.tar" "$TEST_DIR/st"
create_tar "$TEST_DIR/st-new.tar" "$TEST_DIR/stm"
./tar-diff "$TEST_DIR/st-old.tar.gz" "$TEST_DIR/st-new.tar.bz2" "$TEST_DIR/st.tardiff"
./tar-patch "$TEST_DIR/st.tardiff" "$TEST_DIR/st" - | cmp - "$TEST_DIR/st-new.tar"

echo OK cli-errors-tar-diff-tar-patch

cleanup () {
	rm -rf "$TEST_DIR"
}
trap cleanup EXIT
