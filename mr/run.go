package mr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// 运行期目录布局（TempRoot 默认 $TMPDIR/mr-jobs）：
//
//	<tempRoot>/mr-jobs/<jobID>/
//	  run-<随机后缀>/                  本次尝试的独立工作区
//	    map/attempt-<尝试号>/map-<分片号>/spill-*-part-*.bin
//	    reduce/.../part-<分区号>-attempt-<尝试号>.bin（完成后 rename 到 staging）
//	    staging/part-r-<分区号>         提交暂存
//
// 失败尝试目录立即删除；作业结束（成功/失败/取消）后整个 run 目录删除；
// 历史遗留 run 目录在作业启动时清理，因此不会被任何后续运行误读。

type mapOutcome struct {
	taskID int
	files  []string
	recs   int64
	pairs  int64
}

type reduceOutcome struct {
	partition int
	path      string
	outRecs   int64
	runs      int64
}

func (e *Engine) run(ctx context.Context, job *Job) (*Result, error) {
	cfg := job.cfg

	// 已提交且指纹一致：安全复用，不重算；指纹不同：明确拒绝。
	if reused, err := readCommitMarker(cfg.OutputDir); err == nil {
		if reused.Fingerprint != job.fingerprint {
			return nil, &ValidationError{Reason: ErrOutputConflict,
				Detail: fmt.Sprintf("%q committed by job %s with a different fingerprint", cfg.OutputDir, reused.JobID)}
		}
		e.infof("job=%s reuse committed output at %s", cfg.ID, cfg.OutputDir)
		stats := reused.Stats
		stats.ReusedCommitted = true
		return &Result{OutputDir: cfg.OutputDir, Stats: stats}, nil
	}

	// 半成品输出：默认拒绝；显式允许覆盖时清空后重跑（幂等，不会混合半成品）。
	if exists, _ := dirExists(cfg.OutputDir); exists {
		if visible, _ := visibleEntries(cfg.OutputDir); len(visible) == 0 {
			// 只有别的（并发）尝试的 .staging-* 私有暂存，不算半成品。
		} else if cfg.OverwriteUncommitted {
			e.infof("job=%s removing uncommitted output %s", cfg.ID, cfg.OutputDir)
			if err := os.RemoveAll(cfg.OutputDir); err != nil {
				return nil, fmt.Errorf("clean output: %w", err)
			}
		} else {
			return nil, &ValidationError{Reason: ErrUncommittedOutput,
				Detail: fmt.Sprintf("%q exists without %s", cfg.OutputDir, successFileName)}
		}
	}

	tempRoot := cfg.TempRoot
	if tempRoot == "" {
		tempRoot = filepath.Join(os.TempDir(), "mr-jobs")
	}
	jobRoot := filepath.Join(tempRoot, cfg.ID)
	if err := os.MkdirAll(jobRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create temp root: %w", err)
	}
	if entries, err := os.ReadDir(jobRoot); err == nil {
		for _, en := range entries {
			if strings.HasPrefix(en.Name(), "run-") {
				_ = os.RemoveAll(filepath.Join(jobRoot, en.Name()))
			}
		}
	}

	runDir := filepath.Join(jobRoot, "run-"+randomSuffix())
	mapDir := filepath.Join(runDir, "map")
	reduceDir := filepath.Join(runDir, "reduce")
	for _, d := range []string{mapDir, reduceDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("create work dir: %w", err)
		}
	}
	// staging 放在输出目录下、与输出同文件系统：结果文件用硬链接原子
	// 提交（不覆盖已存在文件），并发的另一个尝试无法覆盖先提交者；
	// 中断残留的 .staging-<rand> 在启动时按垃圾清理，绝不会被误读。
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	// 注意：这里不能清理其它 .staging-* —— 并发的另一个尝试可能正在使用。
	// 每个尝试使用自己的随机 staging 目录，互不读取；残留只在独占重跑
	// （OverwriteUncommitted 清空输出目录）时被自然移除。
	stagingDir := filepath.Join(cfg.OutputDir, ".staging-"+randomSuffix())
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return nil, fmt.Errorf("create staging dir: %w", err)
	}
	cleanup := func() {
		if err := os.RemoveAll(runDir); err != nil {
			e.infof("job=%s WARN cleanup run dir: %v", cfg.ID, err)
		}
		if err := os.RemoveAll(stagingDir); err != nil {
			e.infof("job=%s WARN cleanup staging dir: %v", cfg.ID, err)
		}
		if entries, err := os.ReadDir(jobRoot); err == nil && len(entries) == 0 {
			_ = os.Remove(jobRoot)
		}
	}

	stats := Stats{MapTasks: len(job.inputFiles), ReduceTasks: cfg.ReducePartitions}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ---------------- Map 阶段 ----------------
	mapResults := make([]*mapOutcome, len(job.inputFiles))
	mapSem := make(chan struct{}, cfg.Parallelism)
	var mapWG sync.WaitGroup
	var failMu sync.Mutex
	var firstErr error
	fail := func(err error) {
		failMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		failMu.Unlock()
	}

	e.infof("job=%s stage=map start tasks=%d parallelism=%d budget=%dB",
		cfg.ID, len(job.inputFiles), cfg.Parallelism, cfg.MapMemoryBytes)

	for i := range job.inputFiles {
		mapWG.Add(1)
		go func(taskID int) {
			defer mapWG.Done()
			select {
			case mapSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-mapSem }()

			var outcome *mapOutcome
			var attemptErr error
			succeeded := false
			for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
				if err := ctx.Err(); err != nil {
					attemptErr = err
					break
				}
				statsAddAttempt(&stats, true)
				files, recs, pairs, err := e.runMapAttempt(ctx, job, taskID, attempt, mapDir)
				if err == nil {
					outcome = &mapOutcome{taskID: taskID, files: files, recs: recs, pairs: pairs}
					e.infof("job=%s map task=%d attempt=%d OK records=%d pairs=%d spillFiles=%d",
						cfg.ID, taskID, attempt, recs, pairs, len(files))
					succeeded = true
					break
				}
				e.infof("job=%s map task=%d attempt=%d FAIL %v", cfg.ID, taskID, attempt, err)
				attemptErr = err
				var te *TaskError
				if errors.As(err, &te) && !te.Retriable() {
					break
				}
			}
			if !succeeded {
				fail(attemptErr)
				return
			}
			mapResults[taskID] = outcome
		}(i)
	}
	mapWG.Wait()
	if firstErr != nil {
		cleanup()
		return nil, normalizeAbort(ctx, firstErr)
	}
	for _, o := range mapResults {
		stats.InputRecords += o.recs
		stats.MapOutputPairs += o.pairs
		stats.MapSpillFiles += len(o.files)
	}

	partRuns := make([][]string, cfg.ReducePartitions)
	for _, o := range mapResults {
		sort.Strings(o.files)
		for _, p := range o.files {
			var runSeq, part int
			if _, err := fmt.Sscanf(filepath.Base(p), "spill-%d-part-%d.bin", &runSeq, &part); err != nil || part >= cfg.ReducePartitions {
				cleanup()
				return nil, fmt.Errorf("%w: internal: bad spill filename %q", ErrTaskFailed, p)
			}
			partRuns[part] = append(partRuns[part], p)
		}
	}

	// ---------------- Reduce 阶段 ----------------
	reduceResults := make([]*reduceOutcome, cfg.ReducePartitions)
	redSem := make(chan struct{}, cfg.Parallelism)
	var redWG sync.WaitGroup
	firstErr = nil

	e.infof("job=%s stage=reduce start partitions=%d parallelism=%d budget=%dB",
		cfg.ID, cfg.ReducePartitions, cfg.Parallelism, cfg.ReduceMemoryBytes)

	for p := 0; p < cfg.ReducePartitions; p++ {
		redWG.Add(1)
		go func(partition int) {
			defer redWG.Done()
			select {
			case redSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-redSem }()

			var outcome *reduceOutcome
			var attemptErr error
			succeeded := false
			for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
				if err := ctx.Err(); err != nil {
					attemptErr = err
					break
				}
				statsAddAttempt(&stats, false)
				path, recs, runs, err := e.runReduceAttempt(ctx, job, partition, attempt, partRuns[partition], reduceDir, stagingDir)
				if err == nil {
					outcome = &reduceOutcome{partition: partition, path: path, outRecs: recs, runs: runs}
					e.infof("job=%s reduce partition=%d attempt=%d OK outRecords=%d runsMerged=%d",
						cfg.ID, partition, attempt, recs, runs)
					succeeded = true
					break
				}
				e.infof("job=%s reduce partition=%d attempt=%d FAIL %v", cfg.ID, partition, attempt, err)
				attemptErr = err
				var te *TaskError
				if errors.As(err, &te) && !te.Retriable() {
					break
				}
			}
			if !succeeded {
				fail(attemptErr)
				return
			}
			reduceResults[partition] = outcome
		}(p)
	}
	redWG.Wait()
	if firstErr != nil {
		cleanup()
		return nil, normalizeAbort(ctx, firstErr)
	}

	for _, o := range reduceResults {
		stats.OutputRecords += o.outRecs
		stats.ReduceRuns += o.runs
	}

	// ---------------- 提交 ----------------
	marker := CommitMarker{
		JobID:       cfg.ID,
		Fingerprint: job.fingerprint,
		Partitions:  cfg.ReducePartitions,
		Mapper:      typeName(cfg.Mapper),
		Reducer:     typeName(cfg.Reducer),
		CreatedAt:   time.Now(),
		Stats:       stats,
	}
	if err := commitOutput(cfg.OutputDir, reduceResults, marker); err != nil {
		cleanup()
		return nil, err
	}
	cleanup()
	e.infof("job=%s COMMITTED output=%s in=%d pairs=%d out=%d spillFiles=%d",
		cfg.ID, cfg.OutputDir, stats.InputRecords, stats.MapOutputPairs, stats.OutputRecords, stats.MapSpillFiles)
	return &Result{OutputDir: cfg.OutputDir, Stats: stats}, nil
}

var statsMu sync.Mutex

func statsAddAttempt(s *Stats, isMap bool) {
	statsMu.Lock()
	defer statsMu.Unlock()
	if isMap {
		s.MapAttempts++
	} else {
		s.ReduceAttempts++
	}
}

func normalizeAbort(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		if errors.Is(err, ErrTaskFailed) {
			return err
		}
	}
	return err
}

func dirExists(p string) (bool, error) {
	st, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return st.IsDir(), nil
}

func visibleEntries(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, en := range entries {
		if !strings.HasPrefix(en.Name(), ".staging-") {
			names = append(names, en.Name())
		}
	}
	return names, nil
}

func readCommitMarker(outDir string) (CommitMarker, error) {
	var m CommitMarker
	data, err := os.ReadFile(filepath.Join(outDir, successFileName))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, err
	}
	return m, nil
}

// commitOutput 将暂存结果 rename 进输出目录，最后原子写入并 fsync _SUCCESS。
// _SUCCESS 落盘前输出目录一律视为半成品；两个尝试并发提交时，
// 已存在 part 文件即判冲突失败，绝不出两份结果。
func commitOutput(outDir string, results []*reduceOutcome, marker CommitMarker) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for _, o := range results {
		dst := filepath.Join(outDir, fmt.Sprintf("part-r-%05d", o.partition))
		// Link 在同文件系统上是原子的，且目标已存在时失败：
		// 两个并发尝试只有一个能提交成功，输出不翻倍。
		if err := os.Link(o.path, dst); err != nil {
			if os.IsExist(err) {
				return fmt.Errorf("%w: %s already exists (another attempt committed?)", ErrOutputConflict, dst)
			}
			return err
		}
	}
	markerData, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	mf, err := os.OpenFile(filepath.Join(outDir, successFileName),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := mf.Write(markerData); err != nil {
		mf.Close()
		return err
	}
	if err := mf.Sync(); err != nil {
		mf.Close()
		return err
	}
	if err := mf.Close(); err != nil {
		return err
	}
	return syncDir(outDir)
}
