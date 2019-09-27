package adaptor

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
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
	processid := os.Getegid()
	cpuUsage, memUsage, err := getCpuAndMemUsageRate(processid)
	if err != nil {
		return nil, err
	}
	memCap, availableMem, err := getMemCap()
	if err != nil {
		return nil, err
	}
	return &HardWareInfo{
		cpuUsageRate: cpuUsage,
		memUsageRate: memUsage,
		memCap:       memCap,
		availableMem: availableMem,
	}, nil
}

func getCpuAndMemUsageRate(processid int) (float64, float64, error) {
	var cpu, mem float64
	cmd := exec.Command("ps", "aux")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return cpu, mem, err
	}
	for i := 0; ; i++ {
		line, err := out.ReadString('\n')
		if err != nil {
			return cpu, mem, err
		}
		if i == 0 {
			continue
		}
		tokens := strings.Split(line, " ")
		ft := make([]string, 0)
		for _, t := range tokens {
			if t != "" && t != "\t" {
				ft = append(ft, t)
			}
		}
		pid, err := strconv.Atoi(ft[1])
		if err != nil {
			return cpu, mem, err
		}
		if pid != processid {
			continue
		}
		cpu, err = strconv.ParseFloat(ft[2], 64)
		if err != nil {
			log.Fatal(err)
		}
		mem, err = strconv.ParseFloat(ft[3], 64)
		if err != nil {
			log.Fatal(err)
		}
		return cpu, mem, nil
	}
}

func getMemCap() (float64, float64, error) {
	var memCap, availableMem float64
	var err error
	cmd := exec.Command("free", "")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		return memCap, availableMem, err
	}
	for i := 0; ; i++ {
		line, err := out.ReadString('\n')
		if err != nil {
			return memCap, availableMem, err
		}
		if i == 0 {
			continue
		}
		tokens := strings.Split(line, " ")
		ft := make([]string, 0)
		for j, t := range tokens {
			if t != "" && t != "\t" {
				if j == 0 {
					t = strings.TrimRight(t, ":")
				}
				ft = append(ft, t)
			}
		}
		if ft[0] == "Mem" {
			memCap, err = strconv.ParseFloat(ft[1], 64)
			if err != nil {
				return memCap, availableMem, err
			}
			availableMem, err = strconv.ParseFloat(ft[len(ft)-1], 64)
			if err != nil {
				return memCap, availableMem, err
			}
			break
		}
	}
	return memCap, availableMem, err
}

func (hjPG *HashJoinPG) GetStatistic() (*StatsInfo, error) {
	stats := &StatsInfo{
		nullCounts:       make([]int64, 0),
		NDVs:             make([]int64, 0),
		mostCommonVals:   make([][]types.Datum, 0),
		mostCommonCounts: make([][]int64, 0),
		relTupleNums:     make([]int64, 0),
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
					mostCommonVals := make([]types.Datum, 0)
					mostCommonCounts := make([]int64, 0)
					for _, chk := range chkList {
						for i := 0; i < chk.NumRows(); i++ {
							row := chk.GetRow(i)
							mostCommonVals = append(mostCommonVals, row.GetDatum(0, types.NewFieldType(mysql.TypeLongBlob)))
							mostCommonCounts = append(mostCommonCounts, row.GetInt64(1))
						}
					}
					stats.mostCommonVals = append(stats.mostCommonVals, mostCommonVals)
					stats.mostCommonCounts = append(stats.mostCommonCounts, mostCommonCounts)

					// get joinkeys' statistics info of null counts and get joinkeys' statistics info of NDV
					sql = fmt.Sprint("select distinct_count, null_count from mysql.stats_histograms where table_id = ", innerTblPhyId,
						" and hist_id = ", innerKeyIds[i])
					chkList, err = execQuerySQL(context.Background(), session, sql)
					if err != nil {
						return nil, err
					}
					for _, chk := range chkList {
						for i := 0; i < chk.NumRows(); i++ {
							row := chk.GetRow(i)
							NDV := row.GetInt64(0)
							nullCount := row.GetInt64(1)
							stats.NDVs = append(stats.NDVs, NDV)
							stats.nullCounts = append(stats.nullCounts, nullCount)
						}
					}

					// get relation's number
					sql = fmt.Sprint("select count from mysql.stats_meta where table_id = ", innerTblPhyId)
					chkList, err = execQuerySQL(context.Background(), session, sql)
					for _, chk := range chkList {
						for i := 0; i < chk.NumRows(); i++ {
							row := chk.GetRow(i)
							relTupleNum := row.GetInt64(0)
							stats.relTupleNums = append(stats.relTupleNums, relTupleNum)
						}
					}
				}
			}
		}
	}
	return stats, nil
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
