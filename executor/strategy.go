package executor

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hashtable"
)

//define startegy
type Strategy interface {
	Init(ctx context.Context, e Executor) error
	Exec(ctx context.Context, e Executor, req *chunk.Chunk) error
}

type baseStrategy struct {
	strategyName string
}

//...........................................................
//eg. SharedHTStrategy
type SharedHTStrategy struct {
	baseStrategy
}

func (shHTStra *SharedHTStrategy) Init(ctx context.Context, e Executor) error {
	phExec, ok := e.(*ParallelHashExec)
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

	// set concurrency of buildWorker. Default value is 1.
	phExec.concurrency = 4
	return nil
}

func (shHTStra *SharedHTStrategy) Exec(ctx context.Context, e Executor, req *chunk.Chunk) error {
	fmt.Println("strategy named shared hash table is executing...")
	return nil
}

type MVMapStrategy struct {
	baseStrategy
}

func (mvStra *MVMapStrategy) Init(ctx context.Context, e Executor) error {
	const (
		// estCountMaxFactor defines the factor of estCountMax with maxChunkSize.
		// estCountMax is maxChunkSize * estCountMaxFactor, the maximum threshold of estCount.
		// if estCount is larger than estCountMax, set estCount to estCountMax.
		// Set this threshold to prevent innerEstCount being too large and causing a performance and memory regression.
		estCountMaxFactor = 10 * 1024

		// estCountMinFactor defines the factor of estCountMin with maxChunkSize.
		// estCountMin is maxChunkSize * estCountMinFactor, the minimum threshold of estCount.
		// If estCount is smaller than estCountMin, set estCount to 0.
		// Set this threshold to prevent innerEstCount being too small and causing a performance regression.
		estCountMinFactor = 8

		// estCountDivisor defines the divisor of innerEstCount.
		// Set this divisor to prevent innerEstCount being too large and causing a performance regression.
		estCountDivisor = 8
	)
	phExec, ok := e.(*ParallelHashExec)
	if !ok {
		return errors.New("Executor's type is not matched")
	}
	sctx := e.base().ctx
	estCount := int(phExec.innerEstCount)
	maxChunkSize := sctx.GetSessionVars().MaxChunkSize
	// The estCount from cost model is not quite accurate and we need
	// to avoid that it's too large to consume redundant memory.
	// So I invent a rough protection, firstly divide it by estCountDivisor
	// then set a maximum threshold and a minimum threshold.
	estCount /= estCountDivisor
	if estCount > maxChunkSize*estCountMaxFactor {
		estCount = maxChunkSize * estCountMaxFactor
	}
	if estCount < maxChunkSize*estCountMinFactor {
		estCount = 0
	}
	mvmap := hashtable.NewRowHashMap(estCount)
	phExec.HT = mvmap

	// set concurrency of buildWorker. Default value is 1.
	phExec.concurrency = 1
	return nil
}

func (mvStra *MVMapStrategy) Exec(ctx context.Context, e Executor, req *chunk.Chunk) error {
	fmt.Println("strategy named mvmap is executing...")
	return nil
}
