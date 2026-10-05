package mr

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// 每个测试用例都通过 t.Logf 输出场景名、输入规模与判定结果：
//
//	go test -race -v ./...

func wordCountMap(_ context.Context, _ string, line string, emit Emitter) error {
	for _, w := range strings.Fields(line) {
		w = strings.ToLower(strings.Trim(w, ".,!?;:\"'()[]{}"))
		if w == "" {
			continue
		}
		if err := emit.Emit(w, "1"); err != nil {
			return err
		}
	}
	return nil
}

func sumReduce(_ context.Context, key string, values ValueIterator, emit Emitter) error {
	total := 0
	for {
		v, ok, err := values.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		total += n
	}
	return emit.Emit(key, strconv.Itoa(total))
}

func makeWordCountInput(t *testing.T, dir string, files, lines int) []string {
	t.Helper()
	paths := make([]string, 0, files)
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"}
	for f := 0; f < files; f++ {
		p := filepath.Join(dir, fmt.Sprintf("input-%03d.txt", f))
		fh, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		bw := bufio.NewWriter(fh)
		for l := 0; l < lines; l++ {
			// 确定性输入：行号决定词分布。
			fmt.Fprintf(bw, "%s %s %s\n", words[l%len(words)], words[(l/files)%len(words)], words[(l+f)%len(words)])
		}
		bw.Flush()
		fh.Close()
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

func writeTempInput(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func countWords(t *testing.T, paths []string) map[string]string {
	t.Helper()
	want := map[string]int{}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range strings.Fields(string(data)) {
			w = strings.ToLower(strings.Trim(w, ".,!?;:\"'()[]{}"))
			if w != "" {
				want[w]++
			}
		}
	}
	out := make(map[string]string, len(want))
	for k, v := range want {
		out[k] = strconv.Itoa(v)
	}
	return out
}

func readPartOutputs(t *testing.T, outDir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if !isPartFileName(name) {
			continue
		}
		f, err := os.Open(filepath.Join(outDir, name))
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			k, v, ok := strings.Cut(line, "\t")
			if !ok {
				t.Fatalf("非法输出行: %q", line)
			}
			if _, dup := got[k]; dup {
				t.Fatalf("键 %q 出现在多个分组中", k)
			}
			got[k] = v
		}
		f.Close()
	}
	return got
}

func dumpOutput(t *testing.T, outDir string) string {
	t.Helper()
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	names := []string{}
	for _, e := range entries {
		if isPartFileName(e.Name()) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		data, err := os.ReadFile(filepath.Join(outDir, n))
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString(n)
		b.WriteByte(':')
		b.Write(data)
	}
	return b.String()
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func countFilesWithPrefix(t *testing.T, root, prefix string) int {
	t.Helper()
	n := 0
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.HasPrefix(filepath.Base(path), prefix) {
			n++
		}
		return nil
	})
	return n
}

func findScratch(t *testing.T, root string) string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), scratchDirName) {
			return e.Name()
		}
	}
	return ""
}

func TestValidationErrorsBeforeAnyWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	good := writeTempInput(t, dir, "good.txt", "hello world\n")

	cases := []struct {
		name string
		spec *JobSpec
		want error
	}{
		{"分区数为0", &JobSpec{
			InputFiles: []string{good}, OutputDir: filepath.Join(dir, "a"),
			Partitions: 0, Workers: 1, MaxAttempts: 1, ShuffleBytes: 1024,
			MapFn: wordCountMap, ReduceFn: sumReduce}, ErrInvalidJob},
		{"输入不存在", &JobSpec{
			InputFiles: []string{filepath.Join(dir, "nope.txt")}, OutputDir: filepath.Join(dir, "b"),
			Partitions: 1, Workers: 1, MaxAttempts: 1, ShuffleBytes: 1024,
			MapFn: wordCountMap, ReduceFn: sumReduce}, ErrInputNotFound},
		{"缺少算子", &JobSpec{
			InputFiles: []string{good}, OutputDir: filepath.Join(dir, "c"),
			Partitions: 1, Workers: 1, MaxAttempts: 1, ShuffleBytes: 1024}, ErrInvalidJob},
		{"内存预算过小", &JobSpec{
			InputFiles: []string{good}, OutputDir: filepath.Join(dir, "d"),
			Partitions: 1, Workers: 1, MaxAttempts: 1, ShuffleBytes: 1,
			MapFn: wordCountMap, ReduceFn: sumReduce}, ErrInvalidJob},
		{"worker数非法", &JobSpec{
			InputFiles: []string{good}, OutputDir: filepath.Join(dir, "e"),
			Partitions: 1, Workers: 0, MaxAttempts: 1, ShuffleBytes: 1024,
			MapFn: wordCountMap, ReduceFn: sumReduce}, ErrInvalidJob},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Run(context.Background(), tc.spec)
			pass := errors.Is(err, tc.want)
			t.Logf("场景=作业定义校验(%s) 输入=1文件 判定=%s(err=%v)", tc.name, passLabel(pass), err)
			if !pass {
				t.Fatalf("期望 %v, got %v", tc.want, err)
			}
			if entries, _ := os.ReadDir(tc.spec.OutputDir); len(entries) != 0 {
				t.Fatalf("拒绝作业前不得写出数据")
			}
		})
	}
}

func TestOutputConflictWithForeignFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := writeTempInput(t, dir, "in.txt", "a b c\n")
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTempInput(t, out, "user-data.txt", "do not touch\n")

	spec := &JobSpec{
		Name: "conflict", InputFiles: []string{in}, OutputDir: out,
		Partitions: 1, Workers: 1, MaxAttempts: 1, ShuffleBytes: 1024,
		MapFn: wordCountMap, ReduceFn: sumReduce,
	}
	_, err := Run(context.Background(), spec)
	pass := errors.Is(err, ErrOutputConflict)
	t.Logf("场景=输出位置冲突 输入=1文件 输出目录含用户文件 判定=%s(err=%v)", passLabel(pass), err)
	if !pass {
		t.Fatalf("期望 ErrOutputConflict, got %v", err)
	}
	data, err := os.ReadFile(filepath.Join(out, "user-data.txt"))
	if err != nil || string(data) != "do not touch\n" {
		t.Fatalf("用户文件被改动")
	}
}

func TestRerunAfterUncommittedLeftover(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	in := writeTempInput(t, dir, "in.txt", "alpha beta alpha\n")
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	// 伪造一次“提交到一半”的残留：有 part 文件但没有 _SUCCESS。
	if err := os.WriteFile(filepath.Join(out, "part-0000000000"), []byte("stale\t99\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	spec := &JobSpec{
		Name: "leftover", InputFiles: []string{in}, OutputDir: out,
		Partitions: 1, Workers: 1, MaxAttempts: 1, ShuffleBytes: 1024,
		MapFn: wordCountMap, ReduceFn: sumReduce,
	}
	if _, err := Run(context.Background(), spec); err != nil {
		t.Fatalf("应清理可识别残留并重跑: %v", err)
	}
	got := readPartOutputs(t, out)
	pass := got["alpha"] == "2" && got["beta"] == "1"
	t.Logf("场景=半成品残留后重跑 判定=%s(alpha=%s beta=%s)", passLabel(pass), got["alpha"], got["beta"])
	if !pass {
		t.Fatalf("残留未被清理或结果错误: %v", got)
	}
}

func TestConcurrentRunsSameOutputRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputs := makeWordCountInput(t, dir, 4, 300)
	out := filepath.Join(dir, "out")

	base := func() *JobSpec {
		return &JobSpec{
			Name: "concurrent", InputFiles: inputs, OutputDir: out,
			Partitions: 4, Workers: 1, MaxAttempts: 2, ShuffleBytes: 2048,
			MapFn: wordCountMap, ReduceFn: sumReduce,
		}
	}

	// 手工持有 scratch 锁，模拟另一个进程正在跑同一输出位置。
	scratch := filepath.Join(filepath.Dir(out), scratchDirName+"-"+sanitizeName(filepath.Base(out)))
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireDirLock(scratch)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.release()

	_, err = Run(context.Background(), base())
	pass := errors.Is(err, ErrConcurrentRun)
	t.Logf("场景=同位置并发运行互斥 判定=%s(err=%v)", passLabel(pass), err)
	if !pass {
		t.Fatalf("期望 ErrConcurrentRun, got %v", err)
	}
}

func TestMemoryBoundWithHugeSkewUnderRace(t *testing.T) {
	// 综合压力测试（race 下同样要通过）：单任务小预算 + 大量记录 + 倾斜。
	t.Parallel()
	dir := t.TempDir()
	in := filepath.Join(dir, "big.txt")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	bw := bufio.NewWriter(f)
	for i := 0; i < 20000; i++ {
		if i%10 == 0 {
			fmt.Fprintln(bw, "rare"+strconv.Itoa(i))
		} else {
			fmt.Fprintln(bw, "HOT")
		}
	}
	bw.Flush()
	f.Close()

	out := filepath.Join(dir, "out")
	spec := &JobSpec{
		Name: "bound", InputFiles: []string{in}, OutputDir: out,
		Partitions: 4, Workers: 4, MaxAttempts: 1, ShuffleBytes: 4096,
		MapFn: wordCountMap, ReduceFn: sumReduce,
	}
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	if _, err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	runtime.GC()
	runtime.ReadMemStats(&m2)
	got := readPartOutputs(t, out)
	pass := got["hot"] == "18000"
	t.Logf("场景=内存有界综合压力 输入=20000行 预算=4096B worker=4 判定=%s(hot=%s, 堆增量=%dKB)",
		passLabel(pass), got["hot"], (int(m2.HeapAlloc)-int(m1.HeapAlloc))/1024)
	if !pass {
		t.Fatalf("结果错误: hot=%s", got["hot"])
	}
}

func passLabel(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}

func TestWordCountAcrossConcurrency(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputPaths := makeWordCountInput(t, dir, 8, 400)
	expected := countWords(t, inputPaths)

	for _, workers := range []int{1, 2, 4, 8} {
		for _, parts := range []int{1, 3, 7} {
			workers, parts := workers, parts
			t.Run(fmt.Sprintf("w=%d/p=%d", workers, parts), func(t *testing.T) {
				t.Parallel()
				out := filepath.Join(dir, fmt.Sprintf("out-w%d-p%d", workers, parts))
				spec := &JobSpec{
					Name: "wordcount", InputFiles: inputPaths, OutputDir: out,
					Partitions: parts, Workers: workers, MaxAttempts: 1,
					ShuffleBytes: 1 << 20,
					MapFn:        wordCountMap, ReduceFn: sumReduce,
				}
				res, err := Run(context.Background(), spec)
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				got := readPartOutputs(t, out)
				pass := mapsEqual(got, expected)
				t.Logf("场景=词频统计 输入=%d文件x400行 worker=%d 分区=%d map尝试=%d reduce尝试=%d 判定=%s",
					len(inputPaths), workers, parts, res.MapAttempts, res.ReduceAttempts,
					passLabel(pass))
				if !pass {
					t.Fatalf("w=%d p=%d 输出与期望不符: %d vs %d 个键", workers, parts, len(got), len(expected))
				}
			})
		}
	}
}

func TestDeterminismAcrossRunsAndConcurrency(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputs := makeWordCountInput(t, dir, 6, 250)

	var first string
	for i := 0; i < 3; i++ {
		out := filepath.Join(dir, fmt.Sprintf("run-%d", i))
		spec := &JobSpec{
			Name: "wordcount", InputFiles: inputs, OutputDir: out,
			Partitions: 5, Workers: 1 + i%4, MaxAttempts: 2,
			ShuffleBytes: 4096,
			MapFn:        wordCountMap, ReduceFn: sumReduce,
		}
		if _, err := Run(context.Background(), spec); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		dump := dumpOutput(t, out)
		if first == "" {
			first = dump
		} else if dump != first {
			t.Fatalf("不同并发度/多次运行的原始输出不一致")
		}
	}
	t.Logf("场景=并发度无关确定性 输入=6文件x250行 预算=4096B 运行=3次(worker=1,2,3) 判定=PASS")
}

func TestSpillAndMergeUnderTightBudget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputs := makeWordCountInput(t, dir, maxReduceFanIn*2+3, 60)
	expected := countWords(t, inputs)
	out := filepath.Join(dir, "out")

	var spillsObserved int
	var mu sync.Mutex
	spec := &JobSpec{
		Name: "tight", InputFiles: inputs, OutputDir: out,
		Partitions: 3, Workers: 3, MaxAttempts: 1, ShuffleBytes: 512,
		MapFn:    wordCountMap,
		ReduceFn: sumReduce,
		hooks: &Hooks{
			AfterMap: func(_ context.Context, _, _ int, c MapCollectorView) error {
				mu.Lock()
				spillsObserved += c.Spills()
				mu.Unlock()
				return nil
			},
		},
	}
	res, err := Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := readPartOutputs(t, out)
	pass := mapsEqual(got, expected) && spillsObserved > len(inputs)
	t.Logf("场景=内存限额落盘与分层归并 输入=%d文件x60行 预算=512B fan-in上限=%d spill文件数=%d map尝试=%d 判定=%s",
		len(inputs), maxReduceFanIn, spillsObserved, res.MapAttempts, passLabel(pass))
	if !pass {
		t.Fatalf("结果错误或未发生充分落盘: got=%d want=%d spills=%d", len(got), len(expected), spillsObserved)
	}
	if leftover := findScratch(t, dir); leftover != "" {
		t.Fatalf("作业结束后残留 scratch: %s", leftover)
	}
}

func TestSkewedKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	hot := 5000
	in := filepath.Join(dir, "skew.txt")
	f, err := os.Create(in)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for i := 0; i < hot; i++ {
		fmt.Fprintln(w, "hot")
	}
	for _, wd := range []string{"cold1", "cold2", "cold3"} {
		for i := 0; i < 50; i++ {
			fmt.Fprintln(w, wd)
		}
	}
	w.Flush()
	f.Close()

	out := filepath.Join(dir, "out")
	spec := &JobSpec{
		Name: "skew", InputFiles: []string{in}, OutputDir: out,
		Partitions: 2, Workers: 2, MaxAttempts: 1, ShuffleBytes: 1024,
		MapFn: wordCountMap, ReduceFn: sumReduce,
	}
	if _, err := Run(context.Background(), spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := readPartOutputs(t, out)
	pass := got["hot"] == strconv.Itoa(hot) && got["cold1"] == "50" && got["cold2"] == "50" && got["cold3"] == "50"
	t.Logf("场景=键倾斜 输入=热点键%d条+冷键150条 预算=1024B 判定=%s(hot=%s)", hot, passLabel(pass), got["hot"])
	if !pass {
		t.Fatalf("热点键分组结果错误: %v", got)
	}
}

func TestRetryTransientMapFailureThenSucceed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputs := makeWordCountInput(t, dir, 4, 100)
	expected := countWords(t, inputs)
	out := filepath.Join(dir, "out")

	var mu sync.Mutex
	seen := map[int]int{}
	spec := &JobSpec{
		Name: "retry", InputFiles: inputs, OutputDir: out,
		Partitions: 3, Workers: 2, MaxAttempts: 4, ShuffleBytes: 4096,
		MapFn:    wordCountMap,
		ReduceFn: sumReduce,
		hooks: &Hooks{
			BeforeMap: func(_ context.Context, mapID, attempt int) error {
				mu.Lock()
				defer mu.Unlock()
				if mapID != 1 {
					return nil
				}
				seen[attempt]++
				if attempt == 0 {
					return fmt.Errorf("%w: injected transient io error", ErrTaskAttempt)
				}
				if attempt == 1 {
					panic("injected operator panic")
				}
				return nil
			},
		},
	}
	res, err := Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := readPartOutputs(t, out)
	pass := mapsEqual(got, expected)
	t.Logf("场景=Map失败重试(临时错误+panic) 输入=4文件x100行 最大尝试=4 map尝试=%d 判定=%s",
		res.MapAttempts, passLabel(pass))
	if !pass {
		t.Fatalf("重试后结果不一致")
	}
	if res.MapAttempts != res.MapTasks+2 {
		t.Fatalf("预期仅多出2次尝试, got %d vs tasks %d", res.MapAttempts, res.MapTasks)
	}
}

func TestAttemptBudgetExhaustedErrorType(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputs := makeWordCountInput(t, dir, 2, 10)
	out := filepath.Join(dir, "out")
	spec := &JobSpec{
		Name: "alwaysfail", InputFiles: inputs, OutputDir: out,
		Partitions: 2, Workers: 1, MaxAttempts: 2, ShuffleBytes: 4096,
		MapFn: func(context.Context, string, string, Emitter) error {
			panic("permanent failure")
		},
		ReduceFn: sumReduce,
	}
	_, err := Run(context.Background(), spec)
	pass := errors.Is(err, ErrAttemptBudgetExhausted) && errors.Is(err, ErrOperatorPanic)
	t.Logf("场景=重试耗尽错误归一 输入=2文件x10行 判定=%s(err=%v)", passLabel(pass), err)
	if !pass {
		t.Fatalf("期望 ErrAttemptBudgetExhausted+ErrOperatorPanic, got %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 2 {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("失败后残留文件: %v", names)
	}
}

func TestSimulatedCrashAndIdempotentRerun(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inputs := makeWordCountInput(t, dir, 4, 80)
	out := filepath.Join(dir, "out")
	expected := countWords(t, inputs)

	mkSpec := func() *JobSpec {
		return &JobSpec{
			Name: "crash", InputFiles: inputs, OutputDir: out,
			Partitions: 3, Workers: 2, MaxAttempts: 3, ShuffleBytes: 2048,
			MapFn: wordCountMap, ReduceFn: sumReduce,
			hooks: &Hooks{
				CrashMapAttempts:    map[AttemptRef]bool{{TaskID: 0, Attempt: 0}: true},
				CrashReduceAttempts: map[AttemptRef]bool{{TaskID: 0, Attempt: 0}: true},
			},
		}
	}
	res, err := Run(context.Background(), mkSpec())
	if err != nil {
		t.Fatalf("带重试的运行应成功: %v", err)
	}
	got := readPartOutputs(t, out)
	pass := mapsEqual(got, expected)
	t.Logf("场景=中途崩溃重试 map尝试=%d(任务%d) reduce尝试=%d(任务%d) 判定=%s",
		res.MapAttempts, res.MapTasks, res.ReduceAttempts, res.ReduceTasks, passLabel(pass))
	if !pass {
		t.Fatalf("崩溃重试后结果不正确")
	}
	if res.MapAttempts <= res.MapTasks || res.ReduceAttempts <= res.ReduceTasks {
		t.Fatalf("预期至少一次 map/reduce 重试: %+v", res)
	}

	res2, err := Run(context.Background(), mkSpec())
	if err != nil {
		t.Fatalf("重跑: %v", err)
	}
	pass = res2.Reused && mapsEqual(readPartOutputs(t, out), expected)
	t.Logf("场景=同位置幂等重跑 复用=%v 判定=%s", res2.Reused, passLabel(pass))
	if !pass {
		t.Fatalf("重跑未安全复用已提交结果")
	}

	changed := writeTempInput(t, dir, "input-changed.txt", "totally different words here\n")
	spec3 := &JobSpec{
		Name: "crash", InputFiles: []string{changed}, OutputDir: out,
		Partitions: 3, Workers: 1, MaxAttempts: 1, ShuffleBytes: 2048,
		MapFn: wordCountMap, ReduceFn: sumReduce,
	}
	_, err = Run(context.Background(), spec3)
	t.Logf("场景=输入变更拒绝复用 判定=%s(err=%v)", passLabel(errors.Is(err, ErrFingerprintMismatch)), err)
	if !errors.Is(err, ErrFingerprintMismatch) {
		t.Fatalf("期望 ErrFingerprintMismatch, got %v", err)
	}
	if !mapsEqual(readPartOutputs(t, out), expected) {
		t.Fatalf("被拒绝的重跑污染了旧输出")
	}
}
