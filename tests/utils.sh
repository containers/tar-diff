#!/bin/bash

# Common helpers for tar-diff shell tests.
# Source from test scripts using: source tests/utils.sh

use_gnu_tar_if_available() {
    if command -v gtar &>/dev/null; then
        shopt -s expand_aliases
        alias tar=gtar
    fi
    return 0
}

# Windows Detection
if [[ -n "$WINDIR" ]] || [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "msys2" ]]; then
    IS_WINDOWS=true
else
    IS_WINDOWS=false
fi

create_orig () {
    local DIR="$1"

    mkdir -p "$DIR"
    pushd "$DIR" &> /dev/null

    mkdir data
    mkdir data/dir1
    mkdir data/dir2
    echo foo > data/dir1/foo.txt
    echo bar > data/dir1/bar.txt
    echo movedata > data/dir1/move.txt
    
    # Skip broken symlink on Windows: tar refuses to archive symlinks with non-existent targets
    if [[ "$IS_WINDOWS" != true ]]; then
        ln_soft not-exist data/broken
    fi
    ln_soft foo.txt data/dir1/symlink
    ln_hard data/dir1/foo.txt data/dir1/hardlink


    echo "PART1" > data/sparse
    dd if=/dev/zero of=data/sparse bs=1k count=1 seek=1024 conv=notrunc &> /dev/null
    echo "PART2" >> data/sparse

    popd &> /dev/null
}

modify_orig () {
    local DIR="$1"
    local SRC="$2"

    mkdir -p "$DIR"
    # Extract old data
    tar xf "$SRC" -C "$DIR"
    pushd "$DIR" &> /dev/null

    # Modify it
    echo newdata > data/newfile
    mv data/dir1/move.txt data/dir2/move.txt

    echo bar >> data/dir1/bar.txt
    mv data/dir1/bar.txt data/dir1/bar.TXT # Rename we should pick up
    ln_hard data/dir1/foo.txt data/dir1/hardlink2

    popd &> /dev/null
}

compress_tar () {
    local FILE="$1"
    gzip -k "$FILE"
    bzip2 -k "$FILE"
}

create_tar () {
    local FILE="$1"
    local DIR="$2"
    tar cf "$FILE" --sparse -C "$DIR" data
    compress_tar "$FILE"
}

ln_soft() {
    local target="$1"
    local link="$2"

    if [[ "$IS_WINDOWS" == true ]]; then
        touch "${target}"
        powershell -Command "New-Item -ItemType SymbolicLink -Path '${link}' -Target '${target}' -Force"
    else
        ln -s "${target}" "${link}"
    fi
}

ln_hard() {
    local source="$1"
    local link="$2"

    if [[ "$IS_WINDOWS" == true ]]; then
        powershell -Command "New-Item -ItemType HardLink -Path '${link}' -Value '${source}' -Force"
    else
        ln "${source}" "${link}"
    fi
}
