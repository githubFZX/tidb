package adaptor

import (
	"fmt"
	"github.com/pingcap/tidb/types"
)

type StatsInfo struct {
	nullCounts       []int64
	NDVs             []int64
	mostCommonVals   [][]types.Datum
	mostCommonCounts [][]int64
	relTupleNums     []int64

	//The histogram information of join keys.
	//...
}

type HardWareInfo struct {
	cpuUsageRate float64
	memUsageRate float64
	memCap       float64
	availableMem float64
	//other fields...
	//...
}

//define scene
type Scene interface {
	CompareTo(scene Scene) bool
}

type baseScene struct {
	statsInfo    *StatsInfo
	hardwareInfo *HardWareInfo
}

//...........................................................
//HashJoinScene implements interface Scene
type HashJoinScene struct {
	baseScene

	sceneName string

	balanceDegree []float32
	memUsageRate  []float32
	cpuUsageRate  []float32
}

func (hs *HashJoinScene) CompareTo(scene Scene) bool {
	fmt.Println("compare our own scene with scene lib...")
	tempHS, ok := scene.(*HashJoinScene)
	if ok {
		fmt.Println(tempHS)
	}
	return false
}
