package hashtable

import (
	"bytes"
	"sync"
)

const (
	bucketCnt = 8
	valueCnt  = 2

	loadFactor = 20
)

// hmap is the hashmap
type hmap struct {
	hmaplock  sync.Mutex // protect variable count and noverflow
	count     uint64     // the number of live cells
	B         uint64     // 2^B bucket
	noverflow uint64     // the number of overflow buckets

	buckets []*bstate // bucket array
}

// some state message for the bmap chain
type bstate struct {
	bucketlock sync.Mutex

	bcount uint64
	//bucket chain's first node
	firstb *bmap

	// some state message used for insert
	insertb *bmap
	inserti int
}

// bmap is the bucket
type bmap struct {
	tophash  [bucketCnt]uint8   // used for accelerate search
	keys     [bucketCnt][]byte  // key field
	values   [bucketCnt]*vstate // value field
	overflow *bmap              //point to overflow bucket of this bucket
}

// some state message used for accelerate insert value into value chain
type vstate struct {
	firstv *value

	insertv *value
	inserti int
}

// value node
type value struct {
	vals [valueCnt][]byte // value array
	next *value           // point to next value node
}

func overLoadFactor(count int64, B uint64) bool {
	nbukcets := uint64(1) << B
	if float64(nbukcets)*loadFactor < float64(count) {
		return true
	}
	return false
}

func newvalue() *value {
	v := new(value)
	for i := range v.vals {
		v.vals[i] = make([]byte, 0)
	}
	v.next = nil
	return v
}

func newvstate() *vstate {
	vs := new(vstate)
	vs.firstv = newvalue()
	vs.insertv = vs.firstv
	vs.inserti = 0
	return vs
}

// get all val from value chain
func (vs *vstate) getAllvals() [][]byte {
	vals := make([][]byte, 0)
	firstv := vs.firstv
	v := firstv
valuebucketloop:
	for {
		for i := 0; i < valueCnt; i++ {
			if v == vs.insertv && i == vs.inserti {
				break valuebucketloop
			}
			vals = append(vals, v.vals[i])
		}
		// this code is not necessary, because we always break in above place
		if v.next == nil {
			break valuebucketloop
		}
		v = v.next
	}
	return vals
}

func newbmap() *bmap {
	b := new(bmap)

	for i := range b.keys {
		k := make([]byte, 0)
		b.keys[i] = k
	}
	for i := range b.values {
		b.values[i] = newvstate()
	}
	b.overflow = nil
	return b
}

// create and initiate the hmap
func NewMap(hint int64) *hmap {
	h := new(hmap)

	//according to hint to find proper B
	B := uint64(0)
	for overLoadFactor(hint, B) {
		B++
	}
	h.B = B

	//initiate bukcets
	h.buckets = make([]*bstate, uint64(1)<<h.B)
	for i := range h.buckets {
		bs := new(bstate)
		b := newbmap()
		bs.firstb = b
		// the first insert position is the first bucket's first position
		bs.insertb = bs.firstb
		bs.inserti = 0
		h.buckets[i] = bs
	}

	return h
}

// insert key, value into b's position i
func (h *hmap) insert(bs *bstate, b *bmap, bi int, k, v []byte, topHash uint8, isNewPos bool) {
	if isNewPos {
		b.tophash[bi] = topHash
		b.keys[bi] = append(b.keys[bi], k...)
	}
	vs := b.values[bi]
	value := vs.insertv
	vi := vs.inserti
	value.vals[vi] = append(value.vals[vi], v...)

	// update message about bstate and vstate we used
	vi++
	if vi >= valueCnt {
		vi = 0
		overflowv := newvalue()
		value.next = overflowv
		// update vstate message
		vs.insertv = overflowv
		vs.inserti = vi
	} else {
		vs.inserti = vi
	}

	bi++
	if bi >= bucketCnt {
		bi = 0
		overflowb := newbmap()
		b.overflow = overflowb
		// update bstate message
		bs.insertb = overflowb
		bs.inserti = bi
		// update number of overflow count
		h.hmaplock.Lock()
		h.noverflow++
		h.hmaplock.Unlock()
	} else {
		bs.inserti = bi
	}

	// update number of live cells
	h.hmaplock.Lock()
	h.count++
	h.hmaplock.Unlock()
	bs.bcount++
}

// put key value pair to map
func (h *hmap) Put(k, v []byte) bool {
	hash := FnvHash64(k)

	// get bucketMask and calculate bucket position
	bucketMask := uint64(1)<<h.B - 1
	bucketN := hash & bucketMask

	// calculate top hash of the key
	topHashMask := (uint64(1)<<8 - 1) << (64 - 8)
	topHash := uint8((hash & topHashMask) >> (64 - 8))

	bs := h.buckets[bucketN]
	// lock the bucket chain we need to use
	bs.bucketlock.Lock()
	defer bs.bucketlock.Unlock()
	firstb := bs.firstb
	b := firstb
	//find the key equals to k to insert, otherwise insert it to new position.
bucketloop:
	for {
		for i := 0; i < bucketCnt; i++ {
			if b == bs.insertb && i == bs.inserti {
				// there is no key same with k, so insert key value pair here directly
				h.insert(bs, b, i, k, v, topHash, true)
				break bucketloop
			}
			if topHash == b.tophash[i] {
				if bytes.Equal(k, b.keys[i]) {
					// find key same with k, so insert key value pair here
					h.insert(bs, b, i, k, v, topHash, false)
					break bucketloop
				}
			}
			continue
		}
		// this condition should not happen. if happened, means insert failed
		if b.overflow == nil {
			return false
		}
		b = b.overflow
	}

	return true
}

func (h *hmap) Get(k []byte) [][]byte {
	var vals [][]byte

	hash := FnvHash64(k)

	// get bucketMask and calculate bucket position
	bucketMask := uint64(1)<<h.B - 1
	bucketN := hash & bucketMask

	// calculate top hash of the key
	topHashMask := (uint64(1)<<8 - 1) << (64 - 8)
	topHash := uint8((hash & topHashMask) >> (64 - 8))

	bs := h.buckets[bucketN]
	b := bs.firstb
	for {
		for i := 0; i < bucketCnt; i++ {
			if topHash != b.tophash[i] {
				continue
			} else if bytes.Equal(k, b.keys[i]) {
				// get all values and append them to vals
				vs := b.values[i]
				vals = vs.getAllvals()
				break
			} else {
				continue
			}
		}
		if b.overflow == nil {
			break
		}
		b = b.overflow
	}
	return vals
}
