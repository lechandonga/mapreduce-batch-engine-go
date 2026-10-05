// 示例：完全本地的词频统计作业。
//
//	go run ./cmd/wordcount -in ./testdata/input -out ./testdata/output \
//	    -partitions 4 -parallelism 2 -map-budget 1048576 -reduce-budget 1048576
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

func main() {
	var (
		in           = flag.String("in", "", "input directory")
		out          = flag.String("out", "", "output directory")
		id           = flag.String("id", "wordcount", "job id")
		partitions   = flag.Int("partitions", 4, "reduce partition count")
		parallelism  = flag.Int("parallelism", 2, "concurrent tasks per stage")
		attempts     = flag.Int("attempts", 3, "max attempts per task")
		mapBudget    = flag.Int("map-budget", 1<<20, "map memory budget in bytes")
		reduceBudget = flag.Int("reduce-budget", 1<<20, "reduce memory budget in bytes")
		overwrite    = flag.Bool("overwrite-uncommitted", false, "replace half-finished output")
	)
	flag.Parse()

	if *in == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: wordcount -in DIR -out DIR [flags]")
		os.Exit(2)
	}

	cfg := mr.JobConfig{
		ID:                   *id,
		InputDir:             *in,
		OutputDir:            *out,
		ReducePartitions:     *partitions,
		Parallelism:          *parallelism,
		MaxAttempts:          *attempts,
		MapMemoryBytes:       *mapBudget,
		ReduceMemoryBytes:    *reduceBudget,
		OverwriteUncommitted: *overwrite,
		Mapper: mr.MapperFunc(func(_, line string, o mr.MapContext) {
			for _, w := range strings.Fields(line) {
				o.Emit(w, "1")
			}
		}),
		Reducer: mr.ReducerFunc(func(key string, ctx mr.ReduceContext) {
			var n int64
			values := ctx.Values()
			for {
				v, ok := values.Next()
				if !ok {
					break
				}
				x, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					continue
				}
				n += x
			}
			ctx.Emit(key, strconv.FormatInt(n, 10))
		}),
	}

	runner, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		fail(err)
	}
	res, err := runner.Run(context.Background())
	if err != nil {
		fail(err)
	}
	fmt.Printf("done: inputRecords=%d pairs=%d outputRecords=%d spillFiles=%d\n",
		res.Stats.InputRecords, res.Stats.MapOutputPairs, res.Stats.OutputRecords, res.Stats.MapSpillFiles)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "job failed:", err)
	os.Exit(1)
}
