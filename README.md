[![CircleCI](https://circleci.com/gh/cosnicolaou/pbzip2.svg?style=svg)](https://circleci.com/gh/cosnicolaou/pbzip2)
![GithubActions](https://github.com/cosnicolaou/pbzip2/actions/workflows/go-macos.yml/badge.svg)
# pbzip2

This package provides parallel and streaming decompression of bzip2 files. It
operates by scanning the file to find each independent bzip2 block and then
uses a modified version of `compress/bzip2` to decompress each block. The
decompressed blocks are then reassembled into their original order and made
available as a stream (via io.Reader).

The API to use the parallel decompressor is simple:

```go
	input, err := os.Open(filepath.Join("testdata", "hello_world.bz2"))
	if err != nil {
		panic(err)
	}
	io.Copy(os.Stdout, bzip2.NewReader(input))
```

The scanner identifies blocks by searching for the magic numbers that denote
the start of a block and the end of the file. Consequently it will be fooled
if these 6 byte sequences occur in the compressed data but the probability of
this happening is very low (P(a specific 6-byte sequence occurring randomly)),
however, given enough data it will happen. Therefore the decompressor
will attempt to merge blocks that fail to decompress, assuming that the
original bzip block was split because of such a false positive. With this
in place it will take two occurrences of the bzip2 block magic number
occurring to break the decompressor. This boils down to the probability
of a specific 6-byte sequence occurring randomly, twice, within about a MB of
data.

There are three components to this package:
1. the scanner
2. the parallel decompressor
3. the modified bzip2 package

The scanner operates as described above but its implementation is complicated
by the fact that bzip blocks are bit aligned, the handling of concatenated
and empty streams etc. The search for the bzip block magic number is implemented
using three lookup tables. The first is a hash lookup of 256 values to quickly
determine if the next byte could possibly contain the start of the magic number.
The other two tables each consist of 32 bit ints that contain all possible
patterns that the 6 byte magic numbers could occur as, thus allowing for them to
be shifted 1..7 bits in the stream.

The parallel decompressor accepts requests to decompress each bzip2 block
concurrently and then reassembles them into a stream allowing for incremental
processing of the decompressed data. The decompressor uses a modified
version of the go builtin compress/bzip2 package to decompress each block
separately. Fortunately those modifictions are straight forward.

So long as the scanning portion is faster than the decompression by some
reasonable factor, and it is run on a multi-core machine, then this approach
will be significantly faster than a serial bzip2 decompressor. Given the coarse
nature of the operations it should scale linearly with the number of cores
available to it. In simple testing using the wikidata entities downloads
(`https://dumps.wikimedia.org/wikidatawiki/entities/20191202/wikidata-20191202-all.json.bz2`)
it does indeed appear to be 8 times faster than the serial version on an 8
core machine.
