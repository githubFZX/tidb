package hashtable

import "github.com/pingcap/tidb/util/chunk"

const (
	initialEntrySliceLen = 64
	maxEntrySliceLen     = 8 * 1024
)

type entry struct {
	ptr  chunk.RowPtr
	next entryAddr
}

type entryStore struct {
	slices [][]entry
}

func (es *entryStore) init() {
	es.slices = [][]entry{make([]entry, 0, initialEntrySliceLen)}
	// Reserve the first empty entry, so entryAddr{} can represent nullEntryAddr.
	reserved := es.put(entry{})
	if reserved != nullEntryAddr {
		panic("entryStore: first entry is not nullEntryAddr")
	}
}

func (es *entryStore) put(e entry) entryAddr {
	sliceIdx := uint32(len(es.slices) - 1)
	slice := es.slices[sliceIdx]
	if len(slice) == cap(slice) {
		size := cap(slice) * 2
		if size >= maxEntrySliceLen {
			size = maxEntrySliceLen
		}
		slice = make([]entry, 0, size)
		es.slices = append(es.slices, slice)
		sliceIdx++
	}
	addr := entryAddr{sliceIdx: sliceIdx, offset: uint32(len(slice))}
	es.slices[sliceIdx] = append(slice, e)
	return addr
}

func (es *entryStore) get(addr entryAddr) entry {
	return es.slices[addr.sliceIdx][addr.offset]
}

type entryAddr struct {
	sliceIdx uint32
	offset   uint32
}

var nullEntryAddr = entryAddr{}

// rowHashMap stores multiple rowPtr of rows for a given key with minimum GC overhead.
// A given key can store multiple values.
// It is not thread-safe, should only be used in one goroutine.
// TODO(fengliyuan): add unit test for this.
type rowHashMap struct {
	entryStore entryStore
	hashTable  map[uint64]entryAddr
	length     int
}

// newRowHashMap creates a new rowHashMap. estCount means the estimated size of the hashMap.
// If unknown, set it to 0.
func newRowHashMap(estCount int) *rowHashMap {
	m := new(rowHashMap)
	m.hashTable = make(map[uint64]entryAddr, estCount)
	m.entryStore.init()
	return m
}

// Put puts the key/rowPtr pairs to the rowHashMap, multiple rowPtrs are stored in a list.
func (m *rowHashMap) Put(hashKey uint64, rowPtr chunk.RowPtr) {
	oldEntryAddr := m.hashTable[hashKey]
	e := entry{
		ptr:  rowPtr,
		next: oldEntryAddr,
	}
	newEntryAddr := m.entryStore.put(e)
	m.hashTable[hashKey] = newEntryAddr
	m.length++
}

// Get gets the values of the "key" and appends them to "values".
func (m *rowHashMap) Get(hashKey uint64) (rowPtrs []chunk.RowPtr) {
	entryAddr := m.hashTable[hashKey]
	for entryAddr != nullEntryAddr {
		e := m.entryStore.get(entryAddr)
		entryAddr = e.next
		rowPtrs = append(rowPtrs, e.ptr)
	}
	// Keep the order of input.
	for i := 0; i < len(rowPtrs)/2; i++ {
		j := len(rowPtrs) - 1 - i
		rowPtrs[i], rowPtrs[j] = rowPtrs[j], rowPtrs[i]
	}
	return
}

// Len returns the number of rowPtrs in the rowHashMap, the number of keys may be less than Len
// if the same key is put more than once.
func (m *rowHashMap) Len() int { return m.length }
