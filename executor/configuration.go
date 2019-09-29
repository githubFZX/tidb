package executor

//..................................................................................
//Define scene of HashJoin
//The scene applies to shared hash table.
var sharedHTScene Scene = &HashJoinScene{
	sceneName: "sharedHTScene",

	balanceDegree: []float64{0, 1},
	memUsageRate:  []float64{0, 1},
	cpuUsageRate:  []float64{0, 1},
}

//..................................................................................
//Define scene library of hashJoin
//Join above scene belongs to hashJoin to hashjoin scene library.
var HashJoinSceneLib []Scene = []Scene{
	sharedHTScene,
}

//..................................................................................
//Define strategy of HashJoin
//Shared hash table startegy.
var sharedHT Strategy = &SharedHTStrategy{
	baseStrategy{
		"SharedHashTable",
	},
}

//..................................................................................
//Define strategy library of HashJoin
//Join above strategy belongs to hashJoin to hashjoin scene library.
var HashJoinStrategyLib []Strategy = []Strategy{
	sharedHT,
}

//..................................................................................
//Define mapper relation between scene and strategy.
var HashJoinMapper map[Scene][]Strategy = map[Scene][]Strategy{
	sharedHTScene: {sharedHT},
}
