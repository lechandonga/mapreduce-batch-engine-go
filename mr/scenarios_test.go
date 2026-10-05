package mr_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

func genSkewInput(t *testing.T, dir string, files, hotPerFile, coldKeys int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < files; i++ {
		var sb strings.Builder
		for j := 0; j < hotPerFile; j++ {
			fmt.Fprintf(&sb, "hot hot-%d\n", j)
		}
		for k := 0; k < coldKeys; k++ {
			fmt.Fprintf(&sb, "cold-%d v\n", (i*7+k)%50)
		}
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%02d.txt", i)), []byte(sb.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func parseCounts(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	got := map[string]int64{}
	for _, line := range readOutput(t, dir) {
		kv := strings.SplitN(line, "\t", 2)
		if len(kv) != 2 {
			t.Fatalf("bad output line %q", line)
		}
		n, err := strconv.ParseInt(kv[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		got[kv[0]] = n
	}
	return got
}

func TestKeySkewAndSpillUnderBudget(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	const files, hot = 8, 5000
	genSkewInput(t, in, files, hot, 200)

	cfg := baseConfig("skew", in, out)
	cfg.Reducer = wcReduce
	cfg.MapMemoryBytes = 64 * 1024
	cfg.Mapper = mr.MapperFunc(func(_, line string, o mr.MapContext) {
		o.Emit(strings.Fields(line)[0], "1")
	})

	r, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("skew job failed: %v", err)
	}
	counts := parseCounts(t, out)
	if counts["hot"] != files*hot {
		t.Fatalf("hot count got=%d want=%d", counts["hot"], files*hot)
	}
	if res.Stats.MapSpillFiles == 0 {
		t.Fatal("expected spill files under small budget")
	}
	t.Logf("scenario=skew+spill input=%dFiles hot=%d/File verdict=hot=%d spillFiles=%d outputKeys=%d",
		files, hot, counts["hot"], res.Stats.MapSpillFiles, len(counts))
}

func TestLargeJobRunsBeyondMemory(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	genSkewInput(t, in, 40, 1500, 100)

	cfg := baseConfig("large", in, out)
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	cfg.Parallelism = 3
	cfg.MapMemoryBytes = 64 * 1024
	cfg.ReduceMemoryBytes = 64 * 1024

	r, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("large job failed: %v", err)
	}
	counts := parseCounts(t, out)
	if counts["hot"] != 40*1500 {
		t.Fatalf("hot got=%d want=%d", counts["hot"], 40*1500)
	}
	t.Logf("scenario=larger-than-memory input=40Files verdict=ok hot=%d spillFiles=%d reduceRuns=%d",
		counts["hot"], res.Stats.MapSpillFiles, res.Stats.ReduceRuns)
}

func TestDeterminismAcrossParallelism(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	genSkewInput(t, in, 10, 800, 300)

	var ref []string
	for _, par := range []int{1, 2, 5, 9} {
		out := filepath.Join(root, fmt.Sprintf("out-%d", par))
		cfg := baseConfig(fmt.Sprintf("det-%d", par), in, out)
		cfg.Mapper, cfg.Reducer = wcMap, wcReduce
		cfg.Parallelism = par
		cfg.ReducePartitions = 7
		cfg.MapMemoryBytes = 128 * 1024
		r, err := mr.NewEngine().Submit(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Run(context.Background()); err != nil {
			t.Fatalf("parallelism=%d failed: %v", par, err)
		}
		got := readOutput(t, out)
		if ref == nil {
			ref = got
			continue
		}
		if len(got) != len(ref) {
			t.Fatalf("parallelism=%d line count %d != ref %d", par, len(got), len(ref))
		}
		for i := range ref {
			if got[i] != ref[i] {
				t.Fatalf("parallelism=%d line %d differs %q vs %q", par, i, got[i], ref[i])
			}
		}
		t.Logf("scenario=determinism input=10Files parallelism=%d verdict=%d output lines identical", par, len(got))
	}

	seen := map[string]string{}
	entries, _ := os.ReadDir(filepath.Join(root, "out-5"))
	for _, en := range entries {
		if !strings.HasPrefix(en.Name(), "part-r-") {
			continue
		}
		f, _ := os.Open(filepath.Join(root, "out-5", en.Name()))
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			k := strings.SplitN(sc.Text(), "\t", 2)[0]
			if prev, ok := seen[k]; ok {
				t.Fatalf("key %q in both %s and %s", k, prev, en.Name())
			}
			seen[k] = en.Name()
		}
		f.Close()
	}
	t.Logf("scenario=single-group-per-key verdict=%d keys each in exactly one partition", len(seen))
}

func TestFailureRetries(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	genSkewInput(t, in, 6, 400, 50)

	cases := []struct {
		name string
		kind mr.FaultHookKind
	}{
		{"panic", mr.FaultPanic},
		{"io", mr.FaultIO},
		{"exit", mr.FaultExit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(root, "out-"+tc.name)
			cfg := baseConfig("retry-"+tc.name, in, out)
			cfg.Mapper, cfg.Reducer = wcMap, wcReduce
			cfg.MaxAttempts = 4
			var failOnce sync.Map
			cfg.FaultHook = func(phase string, taskID, attempt int) mr.FaultHookKind {
				key := fmt.Sprintf("%s:%d", phase, taskID)
				if (phase == "map" && taskID == 3) || (phase == "reduce" && taskID == 2) {
					if _, loaded := failOnce.LoadOrStore(key, true); !loaded {
						return tc.kind
					}
				}
				return mr.FaultNone
			}
			r, err := mr.NewEngine().Submit(cfg)
			if err != nil {
				t.Fatal(err)
			}
			res, err := r.Run(context.Background())
			if err != nil {
				t.Fatalf("expected success after retry: %v", err)
			}
			if res.Stats.MapAttempts <= res.Stats.MapTasks ||
				res.Stats.ReduceAttempts <= res.Stats.ReduceTasks {
				t.Fatalf("expected extra attempts: %+v", res.Stats)
			}
			counts := parseCounts(t, out)
			if counts["hot"] != 6*400 {
				t.Fatalf("hot count after retry got=%d", counts["hot"])
			}
			t.Logf("scenario=retry/%s input=6Files verdict=ok mapAttempts=%d/%d reduceAttempts=%d/%d hot=%d",
				tc.name, res.Stats.MapAttempts, res.Stats.MapTasks,
				res.Stats.ReduceAttempts, res.Stats.ReduceTasks, counts["hot"])
		})
	}
}

func TestRetriesExhausted(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	genSkewInput(t, in, 3, 100, 10)

	cfg := baseConfig("exhausted", in, out)
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	cfg.MaxAttempts = 2
	cfg.FaultHook = func(phase string, taskID, attempt int) mr.FaultHookKind {
		if phase == "map" && taskID == 1 {
			return mr.FaultPanic
		}
		return mr.FaultNone
	}
	r, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Run(context.Background())
	var te *mr.TaskError
	if !errors.As(err, &te) || te.Kind != mr.FailurePanic || te.Attempt != 2 {
		t.Fatalf("expected TaskError(panic, attempt=2), got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, "_SUCCESS")); !os.IsNotExist(statErr) {
		t.Fatal("failed job must not commit _SUCCESS")
	}
	t.Logf("scenario=attempts-exhausted verdict=%v; no commit marker", err)
}

func TestNoLeftoverTempFiles(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	genSkewInput(t, in, 4, 200, 30)

	cfg := baseConfig("cleanup", in, out)
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	r, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	tempRoot := filepath.Join(os.TempDir(), "mr-test-cleanup", "cleanup")
	if _, err := os.Stat(tempRoot); !os.IsNotExist(err) {
		left, _ := filepath.Glob(filepath.Join(tempRoot, "*", "*"))
		t.Fatalf("leftover temp files: %v", left)
	}
	staged, _ := filepath.Glob(filepath.Join(out, ".staging-*"))
	if len(staged) != 0 {
		t.Fatalf("leftover staging: %v", staged)
	}
	t.Logf("scenario=temp-cleanup verdict=temp dir and staging removed")
}

func TestConcurrentSubmissionsSingleCommit(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	genSkewInput(t, in, 5, 300, 40)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg := baseConfig(fmt.Sprintf("conc-%d", i), in, out)
			cfg.Mapper, cfg.Reducer = wcMap, wcReduce
			cfg.Parallelism = 2
			r, err := mr.NewEngine().Submit(cfg)
			if err != nil {
				errs <- err
				return
			}
			_, err = r.Run(context.Background())
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	var success, conflict int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, mr.ErrOutputConflict):
			conflict++
		default:
			t.Fatalf("only success or conflict allowed, got %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("exactly one commit expected, got success=%d conflict=%d", success, conflict)
	}
	var parts []string
	entries, _ := os.ReadDir(out)
	for _, en := range entries {
		if strings.HasPrefix(en.Name(), "part-r-") {
			parts = append(parts, en.Name())
		}
	}
	if len(parts) != 4 {
		t.Fatalf("expected exactly 4 part files (no doubling), got %v", parts)
	}
	t.Logf("scenario=concurrent-single-commit verdict=success=%d conflict=%d partFiles=%d", success, conflict, len(parts))
}

func TestOverwriteUncommittedRerun(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	genSkewInput(t, in, 3, 150, 20)

	newCfg := func() mr.JobConfig {
		cfg := baseConfig("rerun", in, out)
		cfg.Mapper, cfg.Reducer = wcMap, wcReduce
		return cfg
	}
	r, err := mr.NewEngine().Submit(newCfg())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 模拟中断：删掉提交标记并伪造一个半成品 part。
	if err := os.Remove(filepath.Join(out, "_SUCCESS")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "part-r-00002"), []byte("stale\t999\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := mr.NewEngine().Submit(newCfg()); !errors.Is(err, mr.ErrUncommittedOutput) {
		t.Fatalf("expected ErrUncommittedOutput, got %v", err)
	}
	cfg := newCfg()
	cfg.OverwriteUncommitted = true
	r2, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := r2.Run(context.Background())
	if err != nil {
		t.Fatalf("overwrite rerun failed: %v", err)
	}
	counts := parseCounts(t, out)
	if counts["hot"] != 3*150 || counts["stale"] != 0 {
		t.Fatalf("half-finished data mixed in: %v", counts)
	}
	t.Logf("scenario=rerun-after-interrupt verdict=rejected-then-overwrote-clean hot=%d out=%d",
		counts["hot"], res.Stats.OutputRecords)
}
