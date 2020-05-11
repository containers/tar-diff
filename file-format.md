File Format
-----------

A tar-diff file (media type `application/vnd.tar-diff`) consists of a
header, with the fixed bytes:

```
{ 't', 'a', 'r', 'd', 'f', '1', '\n', 0}
```

Followed by a [zstd](https://facebook.github.io/zstd/) compressed
stream, with a sequence of operations, each operation is encoded as
follows:

```
op: 1 byte
size: uint64 encoded as a varint
data: <size> bytes. data is absent for DeltaOpCopy and DeltaOpSeek
```

For varint encoding, see:
https://developers.google.com/protocol-buffers/docs/encoding#varints

Algorithm
---------
 - unpack the first tar file to create a directory tree, which will be
   referenced by the tar-diff.
 - apply the sequence of operations in the tar-diff file in order,
   producing a stream of bytes identical to the second tar file.

Only the content of the reference files is used, all tar metadata is
available in the tardiff. Similarly, only regular files are referenced
by the tardiff, not e.g. symlinks.


Operations
----------

```
DeltaOpData = 0
DeltaOpOpen = 1
DeltaOpCopy = 2
DeltaOpAddData = 3
DeltaOpSeek = 4
```

***DeltaOpData***
Emit the bytes from `<data>` into the output stream.

***DeltaOpOpen***
`<data>` is a the (relative) path to a file within the original
tarball. Set the source for subsequent `DeltaOpCopy` and `DeltaAddData`
operations to this file, and reset the source position to 0.

Tar-diff generates normalized paths with no `.` or `..`  elements,
which this will never point outside the target directory. However, for
security reasons implementations should not rely on this.

***DeltaOpCopy***
Emit `<size>` bytes from the source file to the
output stream, and advance the source position by `<size>`

***DeltaOpAddData***
Read `<size>` bytes from the source file, and advance the source
position by `<size>`. Add these bytes bytewise to the bytes in
`<data>` with unsigned wrapping, and emit the result to the output
stream.

***DeltaOpSeek***
Set the source position to `<size>`
