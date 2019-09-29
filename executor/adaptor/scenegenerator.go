package adaptor

import (
	"fmt"
	"math"
)

//define scene generator
type SceneGenerator interface {
	GenScene(hwInfo *HardWareInfo, statsInfo *StatsInfo) Scene
}

//..................................................................................
//Define our own sceneGenerator HashJoinSG to implements interface SceneGenerator.
type HashJoinSG struct {
	NDV int64
}

func (hjSG *HashJoinSG) GenScene(hwInfo *HardWareInfo, statsInfo *StatsInfo) Scene {
	fmt.Println("analyze hardware information and statistic information and generate our own scene...")
	// analyze the balance of data. calculate variance of mostCommonCounts
	var minBD, maxBD float64
	if len(statsInfo.mostCommonCounts) > 0 {
		balanceDegrees := make([]float64, 0, len(statsInfo.mostCommonCounts))
		for i := range statsInfo.mostCommonCounts {
			if len(statsInfo.mostCommonCounts[i]) > 0 {
				bg := getVariance(statsInfo.mostCommonCounts[i])
				balanceDegrees = append(balanceDegrees, bg)
			}
		}
		if len(balanceDegrees) > 0 {
			minBD = balanceDegrees[0]
			maxBD = balanceDegrees[0]
			for i := 1; i < len(balanceDegrees); i++ {
				if balanceDegrees[i] < minBD {
					minBD = balanceDegrees[i]
				} else if balanceDegrees[i] > maxBD {
					maxBD = balanceDegrees[i]
				}
			}
		} else {
			minBD = math.MaxFloat64
			maxBD = math.MaxFloat64
		}
	}
	// extract the max NDV from the NDVs
	var maxNDV int64
	if len(statsInfo.NDVs) > 0 {
		NDVs := statsInfo.NDVs
		maxNDV = NDVs[0]
		for i := range NDVs {
			if NDVs[i] > maxNDV {
				maxNDV = NDVs[i]
			}
		}
	} else {
		maxNDV = statsInfo.relTupleNums
	}
	hjSG.NDV = maxNDV

	scene := &HashJoinScene{
		baseScene:     baseScene{statsInfo, hwInfo},
		balanceDegree: []float64{minBD, maxBD},
		cpuUsageRate:  []float64{hwInfo.cpuUsageRate, hwInfo.cpuUsageRate},
		memUsageRate:  []float64{hwInfo.memUsageRate, hwInfo.memUsageRate},
	}
	return scene
}

func getVariance(mcvCount []int64) float64 {
	var quadraticSum float64
	var sum float64
	var count int64
	for i := range mcvCount {
		quadraticSum += float64(mcvCount[i] * mcvCount[i])
		sum += float64(mcvCount[i])
		count++
	}
	result := quadraticSum/float64(count) - (sum*sum)/float64(count*count)
	return result
}
