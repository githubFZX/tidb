package hashtable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"hash"
)

// all strategy's conrresponding hashtable must implements interface HasthTable
type HashTable interface {
	Put(k, v []byte) bool
	Get(k []byte) [][]byte
}

func EncodeKeyToByte(k uint64) []byte {
	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf, k)
	return keyBuf
}

func DecodeKeyFromByte(k []byte) uint64 {
	return binary.BigEndian.Uint64(k)
}

func EncodeValToByte(v chunk.RowPtr) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeValFromByte(v []byte) (chunk.RowPtr, error) {
	decoder := gob.NewDecoder(bytes.NewReader(v))
	var val chunk.RowPtr
	err := decoder.Decode(val)
	if err != nil {
		return _, err
	}
	return val, nil
}

// hashContext keeps the needed hash context of a db table in hash join.
type HashContext struct {
	AllTypes  []*types.FieldType
	KeyColIdx []int
	H         hash.Hash64
	Buf       []byte
}

type HashContainer struct {
	Records *chunk.List
	HT      HashTable

	SC   *stmtctx.StatementContext
	HCtx *HashContext
}

// matchJoinKey checks if join keys of buildRow and probeRow are logically equal.
func (c *HashContainer) matchJoinKey(buildRow, probeRow chunk.Row, probeHCtx *HashContext) (ok bool, err error) {
	return codec.EqualChunkRow(c.SC,
		buildRow, c.HCtx.AllTypes, c.HCtx.KeyColIdx,
		probeRow, probeHCtx.AllTypes, probeHCtx.KeyColIdx)
}

func (c *HashContainer) GetMatchedRows(probeRow chunk.Row, hCtx *HashContext) (matched []chunk.Row, err error) {
	hasNull, key, err := c.GetJoinKeyFromChkRow(c.SC, probeRow, hCtx)
	if err != nil || hasNull {
		return
	}
	k := EncodeKeyToByte(key)
	innerPtrs := c.HT.Get(k)
	if len(innerPtrs) == 0 {
		return
	}
	matched = make([]chunk.Row, 0, len(innerPtrs))
	for _, ptr := range innerPtrs {
		v, err := DecodeValFromByte(ptr)
		if err != nil {
			return
		}
		matchedRow := c.Records.GetRow(v)
		var ok bool
		ok, err = c.matchJoinKey(matchedRow, probeRow, hCtx)
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		matched = append(matched, matchedRow)
	}
	/* TODO(fengliyuan): add test case in this case
	if len(matched) == 0 {
		// noop
	}
	*/
	return
}

// getJoinKeyFromChkRow fetches join keys from row and calculate the hash value.
func (c *HashContainer) GetJoinKeyFromChkRow(sc *stmtctx.StatementContext, row chunk.Row, hCtx *HashContext) (hasNull bool, key uint64, err error) {
	for _, i := range hCtx.KeyColIdx {
		if row.IsNull(i) {
			return true, 0, nil
		}
	}
	hCtx.H.Reset()
	err = codec.HashChunkRow(sc, hCtx.H, row, hCtx.AllTypes, hCtx.KeyColIdx, hCtx.Buf)
	return false, hCtx.H.Sum64(), err
}
