package tar_diff

// This was originally taken from the bsdiff 4 code:

// * Copyright 2003-2005 Colin Percival
// * All rights reserved
// *
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted providing that the following conditions
// * are met:
// * 1. Redistributions of source code must retain the above copyright
// *    notice, this list of conditions and the following disclaimer.
// * 2. Redistributions in binary form must reproduce the above copyright
// *    notice, this list of conditions and the following disclaimer in the
// *    documentation and/or other materials provided with the distribution.
// *
// * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
// * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// * POSSIBILITY OF SUCH DAMAGE.

// Package bsdiff is a binary diff program using suffix sorting.

// Original conversion to go by Gabriel Ochsenhofer at:
// https://github.com/gabstv/go-bsdiff/blob/master/pkg/bsdiff/bsdiff.go
// This was take out and stripped to the miniumum needed for the delta generation

import (
	"bytes"
)

func bsdiff(oldbin, newbin []byte, deltaWriter *deltaWriter) error {
	iii := make([]int, len(oldbin)+1)
	qsufsort(iii, oldbin)

	//var db
	var dblen, eblen, ebpos, slen int

	newsize := len(newbin)
	oldsize := len(oldbin)

	var scan, ln, lastscan, lastpos, lastoffset int
	var oldscore, scsc int
	var pos int

	var s, Sf, lenf, Sb, lenb int
	var overlap, Ss, lens int

	db := make([]byte, 4096)

	for scan < newsize {
		oldscore = 0

		// scsc = scan += len
		scan += ln
		scsc = scan
		for scan < newsize {
			ln = search(iii, oldbin, newbin[scan:], 0, oldsize, &pos)

			for scsc < scan+ln {
				if scsc+lastoffset < oldsize && oldbin[scsc+lastoffset] == newbin[scsc] {
					oldscore++
				}
				scsc++
			}
			if ln == oldscore && ln != 0 {
				break
			}
			if ln > oldscore+8 {
				break
			}
			if scan+lastoffset < oldsize && oldbin[scan+lastoffset] == newbin[scan] {
				oldscore--
			}
			//
			scan++
		}

		if ln != oldscore || scan == newsize {
			s = 0
			Sf = 0
			lenf = 0
			i := 0
			for lastscan+i < scan && lastpos+i < oldsize {
				if oldbin[lastpos+i] == newbin[lastscan+i] {
					s++
				}
				i++
				if s*2-i > Sf*2-lenf {
					Sf = s
					lenf = i
				}
			}

			lenb = 0
			if scan < newsize {
				s = 0
				Sb = 0
				for i = 1; scan >= lastscan+i && pos >= i; i++ {
					if oldbin[pos-i] == newbin[scan-i] {
						s++
					}
					if s*2-i > Sb*2-lenb {
						Sb = s
						lenb = i
					}
				}
			}

			if lastscan+lenf > scan-lenb {
				overlap = (lastscan + lenf) - (scan - lenb)
				s = 0
				Ss = 0
				lens = 0
				for i = 0; i < overlap; i++ {
					if newbin[lastscan+lenf-overlap+i] == oldbin[lastpos+lenf-overlap+i] {
						s++
					}

					if newbin[scan-lenb+i] == oldbin[pos-lenb+i] {
						s--
					}
					if s > Ss {
						Ss = s
						lens = i + 1
					}
				}

				lenf += lens - overlap
				lenb -= lens
			}

			dblen = lenf                              // n bytes diffed
			eblen = (scan - lenb) - (lastscan + lenf) // n bytes extra
			ebpos = lastscan + lenf                   // extra bytes position
			slen = (pos - lenb) - (lastpos + lenf)    // n bytes to skip

			if len(db) < dblen {
				db = make([]byte, dblen+4096)
			}

			for i = 0; i < dblen; i++ {
				db[i] = newbin[lastscan+i] - oldbin[lastpos+i]
			}

			if err := deltaWriter.WriteAddContent(db[:dblen]); err != nil {
				return err
			}

			if err := deltaWriter.WriteContent(newbin[ebpos : ebpos+eblen]); err != nil {
				return err
			}

			if err := deltaWriter.SeekForward(uint64(slen)); err != nil {
				return err
			}

			lastscan = scan - lenb
			lastpos = pos - lenb
			lastoffset = pos - scan
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func search(iii []int, oldbin []byte, newbin []byte, st, en int, pos *int) int {
	var x, y int
	oldsize := len(oldbin)
	newsize := len(newbin)

	if en-st < 2 {
		x = matchlen(oldbin[iii[st]:], newbin)
		y = matchlen(oldbin[iii[en]:], newbin)

		if x > y {
			*pos = iii[st]
			return x
		}
		*pos = iii[en]
		return y
	}

	x = st + (en-st)/2
	cmpln := min(oldsize-iii[x], newsize)
	if bytes.Compare(oldbin[iii[x]:iii[x]+cmpln], newbin[:cmpln]) < 0 {
		return search(iii, oldbin, newbin, x, en, pos)
	}
	return search(iii, oldbin, newbin, st, x, pos)
}

func matchlen(oldbin []byte, newbin []byte) int {
	var i int
	oldsize := len(oldbin)
	newsize := len(newbin)
	for (i < oldsize) && (i < newsize) {
		if oldbin[i] != newbin[i] {
			break
		}
		i++
	}
	return i
}

func qsufsort(iii []int, buf []byte) {
	buckets := make([]int, 256)
	vvv := make([]int, len(iii))
	var i, h, ln int
	bufzise := len(buf)

	for i = 0; i < bufzise; i++ {
		buckets[buf[i]]++
	}

	for i = 1; i < 256; i++ {
		buckets[i] += buckets[i-1]
	}

	for i = 255; i > 0; i-- {
		buckets[i] = buckets[i-1]
	}
	buckets[0] = 0

	for i = 0; i < bufzise; i++ {
		buckets[buf[i]]++
		iii[buckets[buf[i]]] = i
	}
	iii[0] = bufzise

	for i = 0; i < bufzise; i++ {
		vvv[i] = buckets[buf[i]]
	}
	vvv[bufzise] = 0

	for i = 1; i < 256; i++ {
		if buckets[i] == buckets[i-1]+1 {
			iii[buckets[i]] = -1
		}
	}
	iii[0] = -1

	for h = 1; iii[0] != -(bufzise + 1); h += h {
		ln = 0

		i = 0
		for i < bufzise+1 {
			if iii[i] < 0 {
				ln -= iii[i]
				i -= iii[i]
			} else {
				if ln != 0 {
					iii[i-ln] = -ln
				}
				ln = vvv[iii[i]] + 1 - i
				split(iii, vvv, i, ln, h)
				i += ln
				ln = 0
			}
		}
		if ln != 0 {
			iii[i-ln] = -ln
		}
	}

	for i = 0; i < bufzise+1; i++ {
		iii[vvv[i]] = i
	}
}

func split(iii, vvv []int, start, ln, h int) {
	var i, j, k, x int

	if ln < 16 {
		for k = start; k < start+ln; k += j {
			j = 1
			x = vvv[iii[k]+h]
			for i = 1; k+i < start+ln; i++ {
				if vvv[iii[k+i]+h] < x {
					x = vvv[iii[k+i]+h]
					j = 0
				}
				if vvv[iii[k+i]+h] == x {
					iii[k+j], iii[k+i] = iii[k+i], iii[k+j]
					j++
				}
			}
			for i = 0; i < j; i++ {
				vvv[iii[k+i]] = k + j - 1
			}
			if j == 1 {
				iii[k] = -1
			}
		}
		return
	}

	x = vvv[iii[start+(ln/2)]+h]
	var jj, kk int
	for i = start; i < start+ln; i++ {
		if vvv[iii[i]+h] < x {
			jj++
		} else if vvv[iii[i]+h] == x {
			kk++
		}
	}
	jj += start
	kk += jj

	i = start
	j = 0
	k = 0
	for i < jj {
		if vvv[iii[i]+h] < x {
			i++
		} else if vvv[iii[i]+h] == x {
			iii[i], iii[jj+j] = iii[jj+j], iii[i]
			j++
		} else {
			iii[i], iii[kk+k] = iii[kk+k], iii[i]
			k++
		}
	}
	for jj+j < kk {
		if vvv[iii[jj+j]+h] == x {
			j++
		} else {
			iii[jj+j], iii[kk+k] = iii[kk+k], iii[jj+j]
			k++
		}
	}
	if jj > start {
		split(iii, vvv, start, jj-start, h)
	}

	for i = 0; i < kk-jj; i++ {
		vvv[iii[jj+i]] = kk - 1
	}
	if jj == kk-1 {
		iii[jj] = -1
	}

	if start+ln > kk {
		split(iii, vvv, kk, start+ln-kk, h)
	}
}
