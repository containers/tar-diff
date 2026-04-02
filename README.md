[![CI](https://github.com/containers/tar-diff/actions/workflows/ci.yml/badge.svg)](https://github.com/containers/tar-diff/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.25-blue.svg)](https://golang.org/dl/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/containers/tar-diff/graph/badge.svg)](https://codecov.io/gh/containers/tar-diff)

# tar-diff

`tar-diff` is a golang library and set of commandline tools to diff and patch tar files.

`pkg/tar-diff` and the `tar-diff` tool take one or more old tar files (optionally compressed) and a new tar file to generate a single file representing the delta between them (a tardiff file).

`pkg/tar-patch` takes a tardiff file and the uncompressed contents (such as an extracted directory) of the old tar file(s) and reconstructs (binary identically) the new tar file (uncompressed).

## Example
```
$ tar-diff old.tar.gz new.tar.gz delta.tardiff
$ tar xf old.tar.gz -C extracted/
$ tar-patch delta.tardiff extracted/ reconstructed.tar
$ zcat new.tar.gz | shasum
$ shasum reconstructed.tar
```

## Multi-file example

It is sometimes useful to have multiple sources for delta information, such as for example when the
sources are container image layers. In this case, you need to provide the old tar files in
the order they will be extracted when applying:

```
$ tar-diff layer1.tar layer2.tar layer3.tar new-layer.tar delta.tardiff
$ tar xf layer1.tar -C extracted/
$ tar xf layer2.tar -C extracted/
$ tar xf layer3.tar -C extracted/
$ tar-patch delta.tardiff extracted/ reconstructed.tar
```

This handles the case where a file in a later tar file overwrites another.

### Partial extraction with prefix filtering

If you only plan to extract certain directories from the old tar files on the target system,
you can use `--source-prefix` to restrict which files can be used as delta sources:

```
$ tar-diff --source-prefix=blobs/ --source-prefix=config/ old.tar new.tar delta.tardiff
$ tar xf old.tar blobs/ config/ -C extracted/
$ tar-patch delta.tardiff extracted/ reconstructed.tar
```

This ensures the delta only references files that will be available in the extracted directory.

This is particularly useful for e.g. bootc images, where only the files in the ostree repo
will be available on the system. For that case you would run tar-diff with
`--source-prefix=sysroot/ostree/repo/objects/`

## Build requirements

- golang >= 1.25 (see [`go.mod`](go.mod))
- `make`
- `tar`
- `diffutils`, `bzip2`, `gzip` (for tests)

## Runtime dependencies

None. The built binaries are self-contained.


The main use case for `tar-diff` is for more efficient distribution of [OCI images](https://github.com/opencontainers/image-spec).
These images are typically transferred as compressed tar files, but the content is referred to and validated by the checksum of
the uncompressed content. This makes it possible to use an extracted earlier version of an image in combination with a tardiff file
to reconstruct and validate the current version of the image.

Delta compression is based on [bsdiff](http://www.daemonology.net/bsdiff/) and [zstd compression](https://facebook.github.io/zstd/).

The `tar-diff` file format is described in [file-format.md](file-format.md).

## License

`tar-diff` is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE) for the full license text.
