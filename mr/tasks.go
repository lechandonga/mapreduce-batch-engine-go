package mr

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// 单次 reduce 归并允许同时打开的 run 数。
// 每路固定约 64KB 读缓冲 + 一帧（≤ maxRecordBytes 硬上限），
// run 更多时先分段归并，因此 FD 与内存始终有明确上限。
const maxMergeFanIn = 32

// mapEmitter 单任务单 goroutine 使用，无需加锁。
type mapEmitter struct {
	buf        *spillBuffer
	taskID     int32
	seq        int64
	dir        string
	runSeq     int
	parts      int
	spillPaths []string
}

func (m *mapEmitter) Emit(key, value string) {
	r := rec{key: key, value: value, sourceMap: m.taskID, seq: m.seq,
		size: estimateSize(key, value)}
	m.seq++
	if m.buf.Add(r) {
		paths, err := m.buf.Spill(m.dir, m.runSeq, m.parts)
		if err != nil {
			panicSpill(err)
		}
		m.spillPaths = append(m.spillPaths, paths...)
		m.runSeq++
		// 预算此时已清空；记录本身仍可能超过预算，直接落一个单记录 run。
		if m.buf.Add(r) {
			paths2, err2 := m.buf.Spill(m.dir, m.runSeq, m.parts)
			if err2 != nil {
				panicSpill(err2)
			}
			m.spillPaths = append(m.spillPaths, paths2...)
			m.runSeq++
		}
	}
}

type spillPanic struct{ err error }

func panicSpill(err error) { panic(spillPanic{err: err}) }

// runMapAttempt 执行一次 map 尝试。每次尝试使用全新目录；
// 失败（panic/IO/取消/注入故障）时整个尝试目录被删除，不留下半成品。
func (e *Engine) runMapAttempt(ctx context.Context, job *Job, taskID, attempt int, baseDir string) (files []string, inputRecords, pairs int64, err error) {
	cfg := job.cfg
	attemptDir := filepath.Join(baseDir, fmt.Sprintf("attempt-%02d", attempt), fmt.Sprintf("map-%05d", taskID))
	if err := os.MkdirAll(attemptDir, 0o700); err != nil {
		return nil, 0, 0, taskErr("map", taskID, attempt, FailureIO, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(attemptDir)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			if sp, ok := r.(spillPanic); ok {
				err = taskErr("map", taskID, attempt, FailureIO, sp.err)
			} else {
				err = taskErr("map", taskID, attempt, FailurePanic, fmt.Errorf("mapper panic: %v", r))
			}
			files = nil
		}
	}()

	if injected := injectFault(ctx, cfg.FaultHook, "map", taskID, attempt, attemptDir); injected != nil {
		return nil, 0, 0, injected
	}

	rel := job.inputFiles[taskID]
	in, err := os.Open(filepath.Join(cfg.InputDir, filepath.FromSlash(rel)))
	if err != nil {
		return nil, 0, 0, taskErr("map", taskID, attempt, FailureIO, err)
	}
	defer in.Close()

	emitter := &mapEmitter{
		buf:    newSpillBuffer(cfg.MapMemoryBytes),
		taskID: int32(taskID),
		dir:    attemptDir,
		parts:  cfg.ReducePartitions,
	}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), maxRecordBytes)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, taskErr("map", taskID, attempt, FailureCanceled, err)
		}
		line := scanner.Text()
		inputRecords++
		cfg.Mapper.Map(rel, line, emitter)
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, 0, taskErr("map", taskID, attempt, FailureIO, fmt.Errorf("read %s: %w", rel, err))
	}

	if emitter.buf.Len() > 0 {
		paths, serr := emitter.buf.Spill(attemptDir, emitter.runSeq, cfg.ReducePartitions)
		if serr != nil {
			return nil, 0, 0, taskErr("map", taskID, attempt, FailureIO, serr)
		}
		emitter.spillPaths = append(emitter.spillPaths, paths...)
	}
	pairs = emitter.seq

	if err := syncDir(attemptDir); err != nil {
		return nil, 0, 0, taskErr("map", taskID, attempt, FailureIO, err)
	}
	committed = true
	return emitter.spillPaths, inputRecords, pairs, nil
}

// reduceContext 流式暴露同键 values 并缓冲输出。
type reduceContext struct {
	iter    *groupIterator
	out     *bufio.Writer
	emitted int64
}

func (c *reduceContext) Values() ValueIterator { return c.iter }

func (c *reduceContext) Emit(key, value string) {
	if err := writeOutputLine(c.out, key, value); err != nil {
		panicSpill(err)
	}
	c.emitted++
}

// groupIterator 在一个键的分组内迭代；跨界时把下一组首条记录回放给归并器。
type groupIterator struct {
	mi        *mergeIterator
	key       string
	first     rec
	haveFirst bool
}

func (g *groupIterator) Next() (string, bool) {
	var r rec
	if g.haveFirst {
		g.haveFirst = false
		return g.first.value, true
	}
	got, ok, err := g.mi.Next()
	r = got
	if err != nil {
		panicSpill(err)
	}
	if !ok {
		return "", false
	}
	if r.key != g.key {
		g.mi.pushBack(r)
		return "", false
	}
	return r.value, true
}

// runReduceAttempt 执行一次 reduce 尝试：
//  1. run 数超过 maxMergeFanIn 时先分段归并（每路固定缓冲）；
//  2. k 路归并按键分组调用 Reducer，逐组流式输出（单条记录受硬上限约束）；
//  3. 结果先写尝试文件，成功后 rename 进 staging，失败目录整体删除。
func (e *Engine) runReduceAttempt(ctx context.Context, job *Job, partition, attempt int, runs []string, reduceBase, stagingDir string) (staged string, outRecords, runsMerged int64, err error) {
	cfg := job.cfg
	attemptDir := filepath.Join(reduceBase, fmt.Sprintf("attempt-%02d", attempt))
	if err := os.MkdirAll(attemptDir, 0o700); err != nil {
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, err)
	}
	finalRuns := runs
	var intermediate []string
	committed := false
	defer func() {
		if !committed {
			for _, p := range intermediate {
				_ = os.Remove(p)
			}
			_ = os.RemoveAll(attemptDir)
			_ = os.Remove(filepath.Join(stagingDir, fmt.Sprintf(".part-r-%05d.attempt-%02d.tmp", partition, attempt)))
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			if sp, ok := r.(spillPanic); ok {
				err = taskErr("reduce", partition, attempt, FailureIO, sp.err)
			} else {
				err = taskErr("reduce", partition, attempt, FailurePanic, fmt.Errorf("reducer panic: %v", r))
			}
		}
	}()

	if injected := injectFault(ctx, cfg.FaultHook, "reduce", partition, attempt, attemptDir); injected != nil {
		return "", 0, 0, injected
	}

	// 分段归并，把输入 run 收敛到 ≤ maxMergeFanIn 个。
	round := 0
	for len(finalRuns) > maxMergeFanIn {
		var next []string
		for start := 0; start < len(finalRuns); start += maxMergeFanIn {
			end := start + maxMergeFanIn
			if end > len(finalRuns) {
				end = len(finalRuns)
			}
			mergedPath := filepath.Join(attemptDir,
				fmt.Sprintf("merged-r%02d-p%05d-%03d.bin", round, partition, start/maxMergeFanIn))
			if err := mergeRunFiles(finalRuns[start:end], mergedPath); err != nil {
				return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, err)
			}
			intermediate = append(intermediate, mergedPath)
			next = append(next, mergedPath)
		}
		finalRuns = next
		round++
	}

	readers := make([]*runReader, 0, len(finalRuns))
	for _, p := range finalRuns {
		rd, oerr := openRun(p)
		if oerr != nil {
			for _, x := range readers {
				x.Close()
			}
			return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, oerr)
		}
		readers = append(readers, rd)
	}
	mi, merr := mergeRuns(readers)
	if merr != nil {
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, merr)
	}

	// 临时输出直接落在 staging（与输出目录同文件系统），提交时用硬链接，
	// 避免跨设备 rename；失败时按尝试名精确删除。
	tmpOut := filepath.Join(stagingDir, fmt.Sprintf(".part-r-%05d.attempt-%02d.tmp", partition, attempt))
	f, oerr := os.OpenFile(tmpOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if oerr != nil {
		mi.Close()
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, oerr)
	}
	bw := bufio.NewWriterSize(f, 64*1024)
	rctx := &reduceContext{out: bw}

	var emitted int64
	for {
		if err := ctx.Err(); err != nil {
			bw.Flush()
			f.Close()
			mi.Close()
			return "", 0, 0, taskErr("reduce", partition, attempt, FailureCanceled, err)
		}
		r, ok, nerr := mi.Next()
		if nerr != nil {
			bw.Flush()
			f.Close()
			mi.Close()
			return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, nerr)
		}
		if !ok {
			break
		}
		gi := &groupIterator{mi: mi, key: r.key, first: r, haveFirst: true}
		rctx.iter = gi
		cfg.Reducer.Reduce(r.key, rctx)
		emitted = rctx.emitted
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		mi.Close()
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		mi.Close()
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, err)
	}
	if err := f.Close(); err != nil {
		mi.Close()
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, err)
	}
	if err := mi.Close(); err != nil {
		return "", 0, 0, taskErr("reduce", partition, attempt, FailureIO, err)
	}

	staged = tmpOut
	for _, p := range intermediate {
		_ = os.Remove(p)
	}
	committed = true
	return staged, emitted, int64(len(runs)), nil
}

func taskErr(phase string, taskID, attempt int, kind FailureKind, cause error) *TaskError {
	return &TaskError{Phase: phase, TaskID: taskID, Attempt: attempt, Kind: kind, Cause: cause}
}

// injectFault 按 FaultHook 注入故障：
//   - FaultPanic：直接 panic，由 recover 归一为 FailurePanic；
//   - FaultIO：返回 FailureIO 类任务错误（模拟临时读写错误）；
//   - FaultExit：模拟执行中途退出——清空尝试目录后返回 FailureIO。
func injectFault(ctx context.Context, hook FaultHook, phase string, taskID, attempt int, attemptDir string) *TaskError {
	if hook == nil {
		return nil
	}
	switch hook(phase, taskID, attempt) {
	case FaultNone:
		return nil
	case FaultPanic:
		panic(fmt.Errorf("injected operator failure at %s task %d attempt %d", phase, taskID, attempt))
	case FaultIO:
		return taskErr(phase, taskID, attempt, FailureIO,
			errors.New("injected transient IO error"))
	case FaultExit:
		_ = os.RemoveAll(attemptDir)
		return taskErr(phase, taskID, attempt, FailureIO,
			errors.New("injected mid-task exit: attempt workspace removed"))
	default:
		return taskErr(phase, taskID, attempt, FailureUnknown,
			fmt.Errorf("unknown injected fault kind for %s task %d", phase, taskID))
	}
}
