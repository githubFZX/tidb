package executor

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
)

type StatsInfo struct {
	nullCounts       []int64
	NDVs             []int64
	mostCommonVals   [][]types.Datum
	mostCommonCounts [][]int64
	relTupleNums     int64

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
	CompareTo(scene Scene) (bool, error)
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

	balanceDegree []float64
	memUsageRate  []float64
	cpuUsageRate  []float64
}

func (hs *HashJoinScene) CompareTo(scene Scene) (bool, error) {
	fmt.Println("compare our own scene with scene lib...")
	hjScene, ok := scene.(*HashJoinScene)
	if !ok {
		return false, errors.Trace(errors.New("Scene's type is not matched."))
	}
	if hjScene.balanceDegree[0] >= hs.balanceDegree[0] && hjScene.balanceDegree[1] <= hs.balanceDegree[1] {
		if hjScene.cpuUsageRate[0] >= hs.cpuUsageRate[0] && hjScene.cpuUsageRate[1] <= hs.cpuUsageRate[1] {
			if hjScene.memUsageRate[0] >= hs.memUsageRate[0] && hjScene.memUsageRate[1] <= hs.memUsageRate[1] {
				return true, nil
			}
		}
	}
	return false, nil
}
