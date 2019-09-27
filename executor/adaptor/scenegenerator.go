package adaptor

import "fmt"

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

	// TODO:need to analyze the statistics
	// analyze the balance of data. calculate variance of mcvFreqs
	mcvCounts := statsInfo.mostCommonCounts
	variance := getVariance(mcvCounts)
	fmt.Println(variance)

	hjSG.NDV = 1000

	scene := &HashJoinScene{
		baseScene:     baseScene{statsInfo, hwInfo},
		balanceDegree: []float32{variance, variance},
		cpuUsageRate:  []float32{hwInfo.cpuUsageRate, hwInfo.cpuUsageRate},
		memUsageRate:  []float32{hwInfo.memUsageRate, hwInfo.memUsageRate},
	}
	return scene
}

func getVariance(mvcFreqs [][]int64) float32 {
	return 0
}
