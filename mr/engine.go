package mr

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// scratchDirName 是输出目录同级的隐藏工作目录（含文件锁、全部中间数据）。
// 它由引擎独占管理：启动时清空、成功或失败后删除，不会留下被下次运行
// 误读的中间文件；也不会污染用户的输出目录。
const scratchDirName = ".mr-scratch"

// Run 提交并执行一个完全本地化的 MapReduce 作业。
//
// 执行语义：
//   - 作业定义先全部校验通过，引擎才会创建或写出任何文件；
//   - 相同输出位置已有已提交结果时：指纹一致则直接复用，不一致则拒绝；
//   - 同一输出位置已有运行实例时：立即以 ErrConcurrentRun 拒绝；
//   - 任务（map/reduce 尝试）失败按 MaxAttempts 重试，只提交成功尝试；
//   - 全部 reduce 成功后，结果才原子地进入输出目录并写入 _SUCCESS，
//     失败时不产生任何最终输出文件。
func Run(ctx context.Context, spec *JobSpec) (*RunResult, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}

	normalized, err := normalizeSpec(spec)
	if err != nil {
		return nil, err
	}
	spec = normalized

	// 1) 输入校验：必须存在且为普通文件。
	spec.InputFiles = sortedDedup(spec.InputFiles)
	for _, in := range spec.InputFiles {
		st, statErr := os.Stat(in)
		if os.IsNotExist(statErr) {
			return nil, jobError("validate", fmtWrap(ErrInputNotFound, "%s", in))
		}
		if statErr != nil {
			return nil, jobError("validate", fmtWrap(ErrInvalidJob, "stat input %s: %v", in, statErr))
		}
		if st.IsDir() {
			return nil, jobError("validate", fmtWrap(ErrInvalidJob, "input %s is a directory, regular file required", in))
		}
	}

	// 2) 输出位置校验：任何写出动作之前必须确认位置可用。
	outAbs, err := filepath.Abs(spec.OutputDir)
	if err != nil {
		return nil, jobError("validate", fmtWrap(ErrInvalidJob, "resolve output: %v", err))
	}
	if err = ensureNoIOPathOverlap(spec.InputFiles, outAbs); err != nil {
		return nil, jobError("validate", err)
	}
	scratch := filepath.Join(filepath.Dir(outAbs), scratchDirName+"-"+sanitizeName(filepath.Base(outAbs)))

	// 3) 输出目录状态：复用 / 清理可识别残留 / 拒绝外来内容。
	existing, err := inspectOutputDir(outAbs)
	if err != nil {
		return nil, jobError("validate", err)
	}

	// 4) 计算输入指纹（需要读输入，但不产生任何输出文件）。
	fp, err := computeFingerprint(spec)
	if err != nil {
		return nil, jobError("fingerprint", err)
	}

	if existing == outputCommitted {
		oldFP, rfErr := parseFingerprintFile(outAbs)
		if rfErr != nil {
			return nil, jobError("validate", fmtWrap(ErrOutputConflict, "committed output unreadable: %v", rfErr))
		}
		if oldFP != fp {
			return nil, jobError("validate", fmtWrap(ErrFingerprintMismatch, "output %s", outAbs))
		}
		return &RunResult{Reused: true, MapTasks: len(spec.InputFiles), ReduceTasks: spec.Partitions}, nil
	}

	// 5) 加锁并准备 scratch。锁目录独立于输出目录，保证输出目录不存在也能互斥。
	if err = os.MkdirAll(scratch, 0o755); err != nil {
		return nil, jobError("prepare", fmtWrap(ErrTaskAttempt, "create scratch: %v", err))
	}
	lock, err := acquireDirLock(scratch)
	if err != nil {
		return nil, jobError("prepare", err)
	}
	locked := true
	defer func() {
		if locked {
			_ = lock.release()
		}
	}()

	// 拿锁后重新检查输出目录，避免在等待窗口中被其他进程提交。
	existing, err = inspectOutputDir(outAbs)
	if err != nil {
		return nil, jobError("validate", err)
	}
	if existing == outputCommitted {
		oldFP, rfErr := parseFingerprintFile(outAbs)
		if rfErr == nil && oldFP == fp {
			return &RunResult{Reused: true, MapTasks: len(spec.InputFiles), ReduceTasks: spec.Partitions}, nil
		}
		return nil, jobError("validate", fmtWrap(ErrConcurrentRun, "output committed concurrently: %s", outAbs))
	}
	if err = cleanRecognizableOutput(outAbs); err != nil {
		return nil, jobError("prepare", err)
	}
	if err = cleanScratch(scratch); err != nil {
		return nil, jobError("prepare", err)
	}

	// 输出目录此刻创建。
	if err = os.MkdirAll(outAbs, 0o755); err != nil {
		return nil, jobError("prepare", fmtWrap(ErrTaskAttempt, "create output: %v", err))
	}

	staging := filepath.Join(scratch, "staging")
	if err = os.MkdirAll(staging, 0o755); err != nil {
		return nil, jobError("prepare", fmtWrap(ErrTaskAttempt, "create staging: %v", err))
	}

	// 6) 执行阶段。失败时清理所有本次写出的痕迹（scratch 全删，输出目录中
	//    尚未提交任何 part，删除空目录或可识别残留）。
	res, runErr := execute(ctx, spec, scratch, staging)
	if runErr != nil {
		_ = lock.release()
		locked = false
		removeAllOrLog(scratch)
		_ = os.Remove(outAbs)
		return nil, runErr
	}

	// 7) 原子提交：part 文件进入输出目录，再写指纹与 _SUCCESS。
	if err = commitOutput(staging, outAbs, fp, spec.Name); err != nil {
		_ = lock.release()
		locked = false
		removeAllOrLog(scratch)
		_ = os.Remove(outAbs)
		return nil, jobError("commit", err)
	}

	_ = lock.release()
	locked = false
	removeAllOrLog(scratch)
	return res, nil
}

type outputState int

const (
	outputMissing outputState = iota
	outputEmpty
	outputRecognizableLeftover
	outputCommitted
)

// inspectOutputDir 分类输出目录。只有“引擎拥有的内容”才允许继续：
// 已成功提交的目录尝试复用；空目录或仅含可识别 part/标记残留的目录允许清理；
// 任何其他文件都视为用户数据冲突，直接拒绝，绝不覆盖。
func inspectOutputDir(dir string) (outputState, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return outputMissing, nil
	}
	if err != nil {
		return 0, fmtWrap(ErrInvalidJob, "read output dir %s: %v", dir, err)
	}
	if len(entries) == 0 {
		return outputEmpty, nil
	}
	var hasSuccess bool
	for _, e := range entries {
		name := e.Name()
		if name == successMarker {
			hasSuccess = true
			continue
		}
		if name == fingerprintFile || isPartFileName(name) {
			continue
		}
		return 0, fmtWrap(ErrOutputConflict, "output dir %s contains unrecognized entry %q", dir, name)
	}
	if hasSuccess {
		return outputCommitted, nil
	}
	return outputRecognizableLeftover, nil
}

func isPartFileName(name string) bool {
	return strings.HasPrefix(name, "part-") && len(name) > len("part-")
}

func cleanRecognizableOutput(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmtWrap(ErrOutputConflict, "read output dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if name == fingerprintFile || name == successMarker || isPartFileName(name) {
			if rmErr := os.RemoveAll(filepath.Join(dir, name)); rmErr != nil {
				return fmtWrap(ErrTaskAttempt, "remove leftover %s: %v", name, rmErr)
			}
			continue
		}
		return fmtWrap(ErrOutputConflict, "refuse to remove unrecognized entry %q", name)
	}
	return nil
}

func cleanScratch(scratch string) error {
	entries, err := os.ReadDir(scratch)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		// 锁通过打开的 fd 持有，与目录项无关，可安全清空其他内容。
		if rmErr := os.RemoveAll(filepath.Join(scratch, e.Name())); rmErr != nil {
			return fmtWrap(ErrTaskAttempt, "clean scratch: %v", rmErr)
		}
	}
	return nil
}

// ensureNoIOPathOverlap 拒绝输入文件位于输出目录内、或与输出目录相同的情况，
// 避免重跑清理输出时误删用户输入。
func ensureNoIOPathOverlap(inputs []string, outAbs string) error {
	for _, in := range inputs {
		inAbs, err := filepath.Abs(in)
		if err != nil {
			return fmtWrap(ErrInvalidJob, "resolve input path: %v", err)
		}
		rel, err := filepath.Rel(outAbs, inAbs)
		if err != nil {
			return fmtWrap(ErrInvalidJob, "compare paths: %v", err)
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, "../")) {
			return fmtWrap(ErrInvalidJob, "input %s must not be inside output dir %s", inAbs, outAbs)
		}
	}
	return nil
}

func normalizeSpec(spec *JobSpec) (*JobSpec, error) {
	cp := *spec
	if cp.Workers == 0 {
		cp.Workers = 1
	}
	if cp.MaxAttempts == 0 {
		cp.MaxAttempts = 1
	}
	abs := make([]string, len(cp.InputFiles))
	for i, in := range cp.InputFiles {
		a, err := filepath.Abs(in)
		if err != nil {
			return nil, jobError("validate", fmtWrap(ErrInvalidJob, "resolve input %s: %v", in, err))
		}
		abs[i] = a
	}
	cp.InputFiles = abs
	out, err := filepath.Abs(cp.OutputDir)
	if err != nil {
		return nil, jobError("validate", fmtWrap(ErrInvalidJob, "resolve output: %v", err))
	}
	cp.OutputDir = out
	sort.Strings(cp.InputFiles)
	return &cp, nil
}

func sortedDedup(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	w := 0
	for i := range out {
		if i > 0 && out[i] == out[i-1] {
			continue
		}
		out[w] = out[i]
		w++
	}
	return out[:w]
}

func sanitizeName(name string) string {
	r := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", " ", "_")
	s := r.Replace(name)
	if s == "" || s == "." {
		return "out"
	}
	return s
}

// execute 运行 map 与 reduce 两阶段。阶段内部任务可并发，
// 阶段之间有确定性屏障（全部 map 成功提交后 reduce 才开始）。
func execute(ctx context.Context, spec *JobSpec, scratch, staging string) (*RunResult, error) {
	numMaps := len(spec.InputFiles)
	res := &RunResult{MapTasks: numMaps, ReduceTasks: spec.Partitions}

	// ---- Map 阶段 ----
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, spec.Workers)
	var wg sync.WaitGroup
	for id := 0; id < numMaps; id++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(mapID int) {
			defer wg.Done()
			defer func() { <-sem }()
			a, err := runTaskWithRetries(ctx, spec, scratch, true, mapID)
			mu.Lock()
			res.MapAttempts += a
			if err != nil && firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}(id)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, jobError("map", firstErr)
	}

	// ---- Shuffle 视图：只读取已提交的 map 输出 ----
	spillSets, err := committedMapSpills(scratch, numMaps)
	if err != nil {
		return nil, jobError("shuffle", err)
	}

	// ---- Reduce 阶段 ----
	firstErr = nil
	for id := 0; id < spec.Partitions; id++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(redID int) {
			defer wg.Done()
			defer func() { <-sem }()
			a, err := runTaskWithRetriesReduce(ctx, spec, scratch, staging, redID, spillSets)
			mu.Lock()
			res.ReduceAttempts += a
			if err != nil && firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}(id)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, jobError("reduce", firstErr)
	}
	return res, nil
}
