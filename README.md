tar-diff
==

`tar-diff` is a golang library and set of commandline tools to diff and patch tar files.

`pkg/tar-diff` and the `tar-diff` takes two (optionally compressed) tar files and generates a single file representing the delta between them.

`pkg/tar-patch` takes a tardiff and the uncompressed contents (such as an extracted directory) of the first tarfile and reconstructs (binary identically) the second tarfile (uncompressed).

Example:
```
$ tar-diff old.tar.gz new.tar.gz delta.tardiff
$ tar xf old.tar.gz -C extracted/
$ tar-patch delta.tardiff extracted/ reconstructed.tar
$ zcat new.tar.gz | shasum
$ shasum reconstructed.tar
```

The main usecase for tar-diff is for more efficient distribution of [OCI images](https://github.com/opencontainers/image-spec).
These images are typically transferred as compressed tar files, but the content is refered to and validated by the checksum of
the uncomressed content. This makes it possible to use an extracted earlier version of and image in combination with a tardiff
to reconstruct and validate the current version of the image.

License
-
tar-diff is licensed under the Apache License, Version 2.0. See
[LICENSE](LICENSE) for the full license text.
