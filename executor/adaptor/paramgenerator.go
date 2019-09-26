package adaptor

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

//define params generator
type ParamGenerator interface {
	GetSystemState() (*HardWareInfo, error)
	GetStatistic() (*StatsInfo, error)
}

//..........................................................................................
//Define our own ParamGenerator HashJoinPG, which implements the interface ParamGenerator.
type HashJoinPG struct {
	Ctx sessionctx.Context
	E   executor.Executor
}

func (hjPG *HashJoinPG) GetSystemState() (*HardWareInfo, error) {
	fmt.Println("get hardware information...")
	return &HardWareInfo{}, nil
}

func (hjPG *HashJoinPG) GetStatistic() (*StatsInfo, error) {
	stats := &StatsInfo{
		mostCommonVals:   make([]interface{}, 0),
		mostCommonCounts: make([]int64, 0),
	}
	if session, ok := hjPG.Ctx.(sqlexec.SQLExecutor); ok {
		if phExec, ok := hjPG.E.(*executor.ParallelHashExec); ok {
			innerChild := phExec.InnerExec
			if innerTbl, ok := innerChild.(*executor.TableReaderExecutor); ok {
				innerTblInfo := innerTbl.GetTable().Meta()
				innerTblPhyId := innerTblInfo.ID
				innerTbleName := innerTblInfo.Name
				// ananlyze specific table
				_, err := session.Execute(context.Background(), fmt.Sprint("analyze table ", innerTbleName))
				if err != nil {
					return nil, err
				}

				innerKeyIds := make([]int64, len(phExec.InnerKeys))
				for i := range innerKeyIds {
					innerKeyIds[i] = phExec.InnerKeys[i].ID
				}
				// get joinkeys' statistics info of mcv.
				for i := range innerKeyIds {
					sql := fmt.Sprint("select value, count from mysql.stats_top_n where table_id = ", innerTblPhyId,
						" and hist_id = ", innerKeyIds[i])
					chkList, err := execQuerySQL(context.Background(), session, sql)
					if err != nil {
						return nil, err
					}
					for _, chk := range chkList {
						for i := 0; i < chk.NumRows(); i++ {
							row := chk.GetRow(i)
							stats.mostCommonVals = append(stats.mostCommonVals, row.GetBytes(3))
							stats.mostCommonCounts = append(stats.mostCommonCounts, row.GetInt64(4))
						}
					}
				}

				// get joinkeys' statistics info of null counts
				// get joinkeys' statistics info of NDV
				// get relation's number
			}
		}
	}
}

func execQuerySQL(ctx context.Context, exec sqlexec.SQLExecutor, sql string) ([]*chunk.Chunk, error) {
	chkList := make([]*chunk.Chunk, 0)
	recordList, err := exec.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	for i := range recordList {
		req := recordList[i].NewChunk()
		err := recordList[i].Next(ctx, req)
		if err != nil {
			return nil, err
		}
		chkList = append(chkList, req)
	}
	return chkList, nil
}
