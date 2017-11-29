// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kvenc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testKvEncoderSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := tikv.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tidb.SetSchemaLease(0)
	dom, err := tidb.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

type testKvEncoderSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testKvEncoderSuite) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testKvEncoderSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testKvEncoderSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func getExpectKvPairs(tkExpect *testkit.TestKit, sql string) []KvPair {
	tkExpect.MustExec("begin")
	tkExpect.MustExec(sql)
	kvPairsExpect := make([]KvPair, 0)
	kv.WalkMemBuffer(tkExpect.Se.Txn().GetMemBuffer(), func(k kv.Key, v []byte) error {
		kvPairsExpect = append(kvPairsExpect, KvPair{Key: k, Val: v})
		return nil
	})
	tkExpect.MustExec("rollback")
	return kvPairsExpect
}

type testCase struct {
	sql string
	// expectEmptyValCnt only check if expectEmptyValCnt > 0
	expectEmptyValCnt int
	// expectKvCnt only check if expectKvCnt > 0
	expectKvCnt        int
	expectAffectedRows int
}

func (s *testKvEncoderSuite) runTestSQL(c *C, tkExpect *testkit.TestKit, encoder KvEncoder, cases []testCase, tableID int64) {
	for _, ca := range cases {
		comment := fmt.Sprintf("sql:%v", ca.sql)
		kvPairs, affectedRows, err := encoder.Encode(ca.sql, tableID)
		c.Assert(err, IsNil, Commentf(comment))

		if ca.expectAffectedRows > 0 {
			c.Assert(affectedRows, Equals, uint64(ca.expectAffectedRows))
		}

		kvPairsExpect := getExpectKvPairs(tkExpect, ca.sql)
		c.Assert(len(kvPairs), Equals, len(kvPairsExpect), Commentf(comment))
		if ca.expectKvCnt > 0 {
			c.Assert(len(kvPairs), Equals, ca.expectKvCnt, Commentf(comment))
		}

		emptyValCount := 0
		for i, kv := range kvPairs {
			expectKey := kvPairsExpect[i].Key
			if bytes.HasPrefix(kv.Key, tablecodec.TablePrefix()) {
				expectKey = tablecodec.ReplaceRecordKeyTableID(expectKey, tableID)
			}
			c.Assert(bytes.Compare(kv.Key, expectKey), Equals, 0, Commentf(comment))
			c.Assert(bytes.Compare(kv.Val, kvPairsExpect[i].Val), Equals, 0, Commentf(comment))

			if len(kv.Val) == 0 {
				emptyValCount++
			}
		}
		if ca.expectEmptyValCnt > 0 {
			c.Assert(emptyValCount, Equals, ca.expectEmptyValCnt, Commentf(comment))
		}
	}
}

func (s *testKvEncoderSuite) TestInsertPkIsHandle(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	defer dom.Close()

	tkExpect := testkit.NewTestKit(c, store)
	tkExpect.MustExec("use test")
	var tableID int64 = 1
	encoder, err := New("test", nil)
	c.Assert(err, IsNil)
	defer encoder.Close()

	schemaSQL := "create table t(id int auto_increment, a char(10), primary key(id))"
	tkExpect.MustExec(schemaSQL)
	err = encoder.ExecDDLSQL(schemaSQL)
	c.Assert(err, IsNil)

	sqls := []testCase{
		{"insert into t values(1, 'test');", 0, 1, 1},
		{"insert into t(a) values('test')", 0, 1, 1},
		{"insert into t(a) values('test'), ('test1')", 0, 2, 2},
		{"insert into t(id, a) values(3, 'test')", 0, 1, 1},
		{"insert into t values(1000000, 'test')", 0, 1, 1},
		{"insert into t(a) values('test')", 0, 1, 1},
		{"insert into t(id, a) values(4, 'test')", 0, 1, 1},
	}

	s.runTestSQL(c, tkExpect, encoder, sqls, tableID)

	schemaSQL = "create table t1(id int auto_increment, a char(10), primary key(id), key a_idx(a))"
	tkExpect.MustExec(schemaSQL)
	err = encoder.ExecDDLSQL(schemaSQL)
	c.Assert(err, IsNil)

	tableID = 2
	sqls = []testCase{
		{"insert into t1 values(1, 'test');", 0, 2, 1},
		{"insert into t1(a) values('test')", 0, 2, 1},
		{"insert into t1(id, a) values(3, 'test')", 0, 2, 1},
		{"insert into t1 values(1000000, 'test')", 0, 2, 1},
		{"insert into t1(a) values('test')", 0, 2, 1},
		{"insert into t1(id, a) values(4, 'test')", 0, 2, 1},
	}

	s.runTestSQL(c, tkExpect, encoder, sqls, tableID)

	schemaSQL = `create table t2(
		id int auto_increment, 
		a char(10), 
		b datetime default NULL, 
		primary key(id), 
		key a_idx(a),
		unique b_idx(b))
		`

	tableID = 3
	tkExpect.MustExec(schemaSQL)
	err = encoder.ExecDDLSQL(schemaSQL)
	c.Assert(err, IsNil)

	sqls = []testCase{
		{"insert into t2(id, a) values(1, 'test')", 0, 3, 1},
		{"insert into t2 values(2, 'test', '2017-11-27 23:59:59')", 0, 3, 1},
		{"insert into t2 values(3, 'test', '2017-11-28 23:59:59')", 0, 3, 1},
	}
	s.runTestSQL(c, tkExpect, encoder, sqls, tableID)
}

func (s *testKvEncoderSuite) TestInsertPkIsNotHandle(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	defer dom.Close()

	tkExpect := testkit.NewTestKit(c, store)
	tkExpect.MustExec("use test")

	var tableID int64 = 1
	encoder, err := New("test", nil)
	c.Assert(err, IsNil)
	defer encoder.Close()

	schemaSQL := `create table t(
		id varchar(20),
		a char(10),
		primary key(id))`
	tkExpect.MustExec(schemaSQL)
	c.Assert(encoder.ExecDDLSQL(schemaSQL), IsNil)

	sqls := []testCase{
		{"insert into t values(1, 'test');", 0, 2, 1},
		{"insert into t(id, a) values(2, 'test')", 0, 2, 1},
		{"insert into t(id, a) values(3, 'test')", 0, 2, 1},
		{"insert into t values(1000000, 'test')", 0, 2, 1},
		{"insert into t(id, a) values(5, 'test')", 0, 2, 1},
		{"insert into t(id, a) values(4, 'test')", 0, 2, 1},
		{"insert into t(id, a) values(6, 'test'), (7, 'test'), (8, 'test')", 0, 6, 3},
	}

	s.runTestSQL(c, tkExpect, encoder, sqls, tableID)
}

func (s *testKvEncoderSuite) TestRetryWithAllocator(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	defer dom.Close()

	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use test")
	alloc := NewAllocator()
	var tableID int64 = 1
	encoder, err := New("test", alloc)
	c.Assert(err, IsNil)
	defer encoder.Close()

	schemaSQL := `create table t(
		id int auto_increment,
		a char(10),
		primary key(id))`
	tk.MustExec(schemaSQL)
	c.Assert(encoder.ExecDDLSQL(schemaSQL), IsNil)

	sqls := []string{
		"insert into t(a) values('test');",
		"insert into t(a) values('test'), ('1'), ('2'), ('3');",
		"insert into t(id, a) values(1000000, 'test')",
		"insert into t(id, a) values(5, 'test')",
		"insert into t(id, a) values(4, 'test')",
	}

	for _, sql := range sqls {
		baseID := alloc.Base()
		kvPairs, _, err1 := encoder.Encode(sql, tableID)
		c.Assert(err1, IsNil, Commentf("sql:%s", sql))
		alloc.Rebase(tableID, baseID, false)
		retryKvPairs, _, err1 := encoder.Encode(sql, tableID)
		c.Assert(err1, IsNil, Commentf("sql:%s", sql))
		c.Assert(len(kvPairs), Equals, len(retryKvPairs))
		for i, kv := range kvPairs {
			c.Assert(bytes.Compare(kv.Key, retryKvPairs[i].Key), Equals, 0, Commentf(sql))
			c.Assert(bytes.Compare(kv.Val, retryKvPairs[i].Val), Equals, 0, Commentf(sql))
		}
	}

	// specify id, it must be the same kv
	sql := "insert into t(id, a) values(5, 'test')"
	kvPairs, _, err := encoder.Encode(sql, tableID)
	c.Assert(err, IsNil, Commentf("sql:%s", sql))
	retryKvPairs, _, err := encoder.Encode(sql, tableID)
	c.Assert(err, IsNil, Commentf("sql:%s", sql))
	c.Assert(len(kvPairs), Equals, len(retryKvPairs))
	for i, kv := range kvPairs {
		c.Assert(bytes.Compare(kv.Key, retryKvPairs[i].Key), Equals, 0, Commentf(sql))
		c.Assert(bytes.Compare(kv.Val, retryKvPairs[i].Val), Equals, 0, Commentf(sql))
	}

	schemaSQL = `create table t1(
		id int auto_increment,
		a char(10),
		b char(10),
		primary key(id),
		KEY idx_a(a),
		unique b_idx(b))`
	tk.MustExec(schemaSQL)
	c.Assert(encoder.ExecDDLSQL(schemaSQL), IsNil)
	tableID = 2

	sqls = []string{
		"insert into t1(a, b) values('test', 'b1');",
		"insert into t1(a, b) values('test', 'b2'), ('1', 'b3'), ('2', 'b4'), ('3', 'b5');",
		"insert into t1(id, a, b) values(1000000, 'test', 'b6')",
		"insert into t1(id, a, b) values(5, 'test', 'b7')",
		"insert into t1(id, a, b) values(4, 'test', 'b8')",
		"insert into t1(a, b) values('test', 'b9');",
	}

	for _, sql := range sqls {
		baseID := alloc.Base()
		kvPairs, _, err1 := encoder.Encode(sql, tableID)
		c.Assert(err1, IsNil, Commentf("sql:%s", sql))
		alloc.Rebase(tableID, baseID, false)
		retryKvPairs, _, err1 := encoder.Encode(sql, tableID)
		c.Assert(err1, IsNil, Commentf("sql:%s", sql))
		c.Assert(len(kvPairs), Equals, len(retryKvPairs))
		for i, kv := range kvPairs {
			c.Assert(bytes.Compare(kv.Key, retryKvPairs[i].Key), Equals, 0, Commentf(sql))
			c.Assert(bytes.Compare(kv.Val, retryKvPairs[i].Val), Equals, 0, Commentf(sql))
		}
	}
}

func (s *testKvEncoderSuite) TestSimpleKeyEncode(c *C) {
	encoder, err := New("test", nil)
	c.Assert(err, IsNil)
	defer encoder.Close()

	schemaSQL := `create table t(
		id int auto_increment, 
		a char(10), 
		b char(10) default NULL, 
		primary key(id), 
		key a_idx(a))
		`
	encoder.ExecDDLSQL(schemaSQL)
	tableID := int64(1)
	indexID := int64(1)

	sql := "insert into t values(1, 'a', 'b')"
	kvPairs, affectedRows, err := encoder.Encode(sql, tableID)
	c.Assert(err, IsNil)
	c.Assert(len(kvPairs), Equals, 2)
	c.Assert(affectedRows, Equals, uint64(1))
	tablePrefix := tablecodec.GenTableRecordPrefix(tableID)
	handle := int64(1)
	expectRecordKey := tablecodec.EncodeRecordKey(tablePrefix, handle)

	indexPrefix := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	expectIdxKey := []byte{}
	expectIdxKey = append(expectIdxKey, []byte(indexPrefix)...)
	expectIdxKey, err = codec.EncodeKey(expectIdxKey, types.NewDatum([]byte("a")))
	c.Assert(err, IsNil)
	expectIdxKey, err = codec.EncodeKey(expectIdxKey, types.NewDatum(handle))
	c.Assert(err, IsNil)

	for _, kv := range kvPairs {
		tID, iID, isRecordKey, err1 := tablecodec.DecodeKeyHead(kv.Key)
		c.Assert(err1, IsNil)
		c.Assert(tID, Equals, tableID)
		if isRecordKey {
			c.Assert(bytes.Compare(kv.Key, expectRecordKey), Equals, 0)
		} else {
			c.Assert(iID, Equals, indexID)
			c.Assert(bytes.Compare(kv.Key, expectIdxKey), Equals, 0)
		}
	}

	// unique index key
	schemaSQL = `create table t1(
		id int auto_increment, 
		a char(10), 
		primary key(id), 
		unique a_idx(a))
		`
	encoder.ExecDDLSQL(schemaSQL)
	tableID = int64(2)
	sql = "insert into t1 values(1, 'a')"
	kvPairs, affectedRows, err = encoder.Encode(sql, tableID)
	c.Assert(err, IsNil)
	c.Assert(len(kvPairs), Equals, 2)
	c.Assert(affectedRows, Equals, uint64(1))

	tablePrefix = tablecodec.GenTableRecordPrefix(tableID)
	handle = int64(1)
	expectRecordKey = tablecodec.EncodeRecordKey(tablePrefix, handle)

	indexPrefix = tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	expectIdxKey = []byte{}
	expectIdxKey = append(expectIdxKey, []byte(indexPrefix)...)
	expectIdxKey, err = codec.EncodeKey(expectIdxKey, types.NewDatum([]byte("a")))
	c.Assert(err, IsNil)

	for _, kv := range kvPairs {
		tID, iID, isRecordKey, err1 := tablecodec.DecodeKeyHead(kv.Key)
		c.Assert(err1, IsNil)
		c.Assert(tID, Equals, tableID)
		if isRecordKey {
			c.Assert(bytes.Compare(kv.Key, expectRecordKey), Equals, 0)
		} else {
			c.Assert(iID, Equals, indexID)
			c.Assert(bytes.Compare(kv.Key, expectIdxKey), Equals, 0)
		}
	}

}
