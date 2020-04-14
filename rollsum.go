package tar_diff

import (
	"hash"
	"hash/crc32"
	"sort"
)

const (
	maxBlobSize = 8192 * 4

	// According to librsync/rollsum.h:
	// "We should make this something other than zero to improve the
	// checksum algorithm: tridge suggests a prime number."
	// apenwarr: I unscientifically tried 0 and 7919, and they both ended up
	// slightly worse than the librsync value of 31 for my arbitrary test data.
	bupCharOffset = 31

	bupBlobBits   = 13
	bupBlobSize   = (1 << bupBlobBits)
	bupWindowBits = 7
	bupWindowSize = (1 << bupWindowBits)
)

type RollsumBlob struct {
	offset int64
	size   int64
	crc32  uint32
}

type Rollsum struct {
	// Current blob
	blobStart int64
	blobSize  int64
	blobCrc   hash.Hash32

	// rolling sum to track when to split blob off
	s1, s2 uint32
	window [bupWindowSize]byte
	wofs   int32

	// Resulting blobs
	header []byte
	blobs  []RollsumBlob
}

// These formulas are based on rollsum.h in the librsync project.
func (r *Rollsum) add(drop byte, add byte) {
	r.s1 += uint32(add) - uint32(drop)
	r.s2 += r.s1 - (bupWindowSize * (uint32(drop) + bupCharOffset))
}

func (r *Rollsum) roll(ch byte) {
	r.blobSize += 1
	r.add(r.window[r.wofs], ch)
	r.window[r.wofs] = ch
	r.wofs = (r.wofs + 1) % bupWindowSize
}

func (r *Rollsum) digest() uint32 {
	return r.s1<<16 | r.s2&0xffff
}

func (r *Rollsum) shouldSplit() bool {
	return r.blobSize == maxBlobSize ||
		(r.s2&(bupBlobSize-1)) == (^uint32(0)&(bupBlobSize-1))
}

func (r *Rollsum) init() {
	r.blobStart = r.blobStart + r.blobSize
	r.blobSize = 0
	r.blobCrc = crc32.NewIEEE()
	r.s1 = bupWindowSize * bupCharOffset
	r.s2 = bupWindowSize * (bupWindowSize - 1) * bupCharOffset
	r.wofs = 0
	for i := range r.window {
		r.window[i] = 0
	}
}

func (r *Rollsum) addBlob() {
	blob := RollsumBlob{offset: r.blobStart, size: r.blobSize, crc32: r.blobCrc.Sum32()}
	r.blobs = append(r.blobs, blob)
	r.init()
}

func NewRollsum() *Rollsum {
	r := new(Rollsum)
	r.header = make([]byte, 0, 16)
	r.blobs = make([]RollsumBlob, 0)
	r.init()
	return r
}

func (r *Rollsum) flush() {
	if r.blobSize > 0 {
		r.addBlob()
	}
}

func (r *Rollsum) GetBlobs() []RollsumBlob {
	r.flush()
	return r.blobs
}

func (r *Rollsum) GetHeader() []byte {
	return r.header
}

func (r *Rollsum) Write(p []byte) (nn int, err error) {
	nn = len(p)
	if nn == 0 {
		return
	}

	// Keep the first cap(header) bytes for type detection
	header := r.header
	if len(header) < cap(header) {
		for i := 0; len(header) < cap(header) && i < len(p); i++ {
			l := len(header)
			header = header[:l+1]
			header[l] = p[i]
		}
		r.header = header
	}

	start := 0
	for i := range p {
		r.roll(p[i])
		if r.shouldSplit() {
			r.blobCrc.Write(p[start : i+1])
			start = i + 1
			r.addBlob()
		}
	}
	if start < nn {
		r.blobCrc.Write(p[start:nn])
	}
	return
}

func makeCrcMap(blobs []RollsumBlob) map[uint32][]*RollsumBlob {
	blobsMap := make(map[uint32][]*RollsumBlob)
	for i := range blobs {
		b := &blobs[i]

		a := blobsMap[b.crc32]
		a = append(a, b)
		blobsMap[b.crc32] = a
	}
	return blobsMap
}

type RollsumMatch struct {
	from *RollsumBlob
	to   *RollsumBlob
}
type RollsumMatches struct {
	matches    []RollsumMatch
	matchRatio int
}

func ComputeRollsumMatches(from []RollsumBlob, to []RollsumBlob) *RollsumMatches {
	fromByCrc := makeCrcMap(from)

	nMatches := 0
	matchSize := int64(0)
	matches := make([]RollsumMatch, 0)

	for i := range to {
		t := &to[i]
		fs := fromByCrc[t.crc32]
		if fs == nil {
			continue
		}

		for j := range fs {
			f := fs[j]
			// If same crc but different length, skip it
			if f.size == t.size {
				// Size and crc matches, assume an exact hit but verify when actually computing delta
				nMatches++
				matchSize += f.size
				matches = append(matches, RollsumMatch{f, t})
				break
			}
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].to.offset < matches[j].to.offset
	})

	return &RollsumMatches{
		matches:    matches,
		matchRatio: nMatches * 100 / len(to),
	}
}
