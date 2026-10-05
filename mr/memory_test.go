package mr_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

// TestMemoryBoundUnderSkew 在作业运行期间高频采样堆分配峰值。
// 数据量（~8MB 文本）是任务内存预算（64KB）的上百倍，若缓冲区无界增长
// 或分组被整体读入内存，峰值会远超下面给定的经验上限。
func TestMemoryBoundUnderSkew(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	genSkewInput(t, in, 12, 10000, 500) // 约 12 万行、数 MB 文本

	cfg := baseConfig("membound", in, out)
	cfg.Reducer = wcReduce
	cfg.Parallelism = 4
	cfg.ReducePartitions = 4
	cfg.MapMemoryBytes = 64 * 1024
	cfg.ReduceMemoryBytes = 64 * 1024
	cfg.Mapper = mr.MapperFunc(func(_, line string, o mr.MapContext) {
		o.Emit(line[:3], "1")
	})

	stop := make(chan struct{})
	var peak uint64
	var sampleWG sync.WaitGroup
	sampleWG.Add(1)
	go func() {
		defer sampleWG.Done()
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
				runtime.ReadMemStats(&m)
				if m.Alloc > peak {
					peak = m.Alloc
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	r, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Run(context.Background())
	close(stop)
	sampleWG.Wait()
	if err != nil {
		t.Fatalf("job failed: %v", err)
	}

	// 4 并发 × (map 预算 + reduce 预算) = 512KB 的任务驻留，
	// 加上 run 缓冲/归并堆/Go 运行时开销，给 16MB 的经验上限；
	// 无界实现下该作业峰值会到数十 MB。
	const limit = 16 * 1024 * 1024
	t.Logf("scenario=memory-bound input=120kLines budget=64KBx4 verdict=peakHeap=%dB limit=%dB out=%d",
		peak, limit, res.Stats.OutputRecords)
	if peak > limit {
		t.Fatalf("heap peak %dB exceeded bound %dB (unbounded buffering?)", peak, limit)
	}
	if n := parseCounts(t, out)["hot"]; n != 12*10000 {
		t.Fatalf("hot count wrong: %d", n)
	}
	if res.Stats.MapSpillFiles < 20 {
		t.Fatalf("expected many spills under tight budget, got %d", res.Stats.MapSpillFiles)
	}
}

var _ = fmt.Sprintf
