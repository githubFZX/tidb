package adaptor

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hashtable"
)

//define startegy
type Strategy interface {
	Init(ctx context.Context, e executor.Executor) error
	Exec(ctx context.Context, e executor.Executor, req *chunk.Chunk) error
}

type baseStrategy struct {
	strategyName string
}

//...........................................................
//eg. SharedHTStrategy
type SharedHTStrategy struct {
	baseStrategy
}

func (shHTStra *SharedHTStrategy) Init(ctx context.Context, e executor.Executor) error {
	phExec, ok := e.(*executor.ParallelHashExec)
	if !ok {
		return errors.Trace(errors.New("Executor's type is not matched."))
	}
	hjAdaptor, ok := phExec.Adaptor.(*HashJoinAdapter)
	if !ok {
		return errors.Trace(errors.New("Adaptor's type is not matched."))
	}
	hjSG, ok := hjAdaptor.sg.(*HashJoinSG)
	if !ok {
		return errors.Trace(errors.New("SceneGenerator's type is not matched."))
	}
	NDV := hjSG.NDV
	// create SharedHT
	sharedHT := hashtable.NewMap(NDV)
	phExec.HT = sharedHT
	return nil
}

func (shHTStra *SharedHTStrategy) Exec(ctx context.Context, e executor.Executor, req *chunk.Chunk) error {
	fmt.Println("strategy named shared hash table is executing...")
	return nil
}
