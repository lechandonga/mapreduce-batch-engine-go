package mr

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// committedMarker 是 map 尝试目录发布成功的唯一标志。
// reducer 只接受含该文件的目录，因此未完成尝试对下游不可见。
const committedMarker = "COMMITTED"

// mapAttemptDir / reduceAttemptDir:
//
//	<scratch>/map/m-<mapID>/attempt-<n>     提交后保留为 .../m-<mapID>/attempt-<n>
//	<scratch>/reduce/r-<redID>/attempt-<n>  提交后把 part 文件发布到 staging
func mapTaskDir(root string, mapID int) string {
	return filepath.Join(root, "map", "m-"+pad10(mapID))
}

func reduceTaskDir(root string, redID int) string {
	return filepath.Join(root, "reduce", "r-"+pad10(redID))
}

// runMapAttempt 执行一次 map 尝试。成功返回时尝试目录已包含 COMMITTED；
// 任何失败都会清理本次尝试目录，重试时从空目录重来，天然不重不丢。
func runMapAttempt(ctx context.Context, spec *JobSpec, root string, mapID int, attempt int) (err error) {
	taskDir := mapTaskDir(root, mapID)
	attemptDir := filepath.Join(taskDir, "attempt-"+pad10(attempt))
	if err = os.MkdirAll(attemptDir, 0o755); err != nil {
		return fmtWrap(ErrTaskAttempt, "mkdir map attempt: %v", err)
	}
	committed := false
	defer func() {
		if !committed && !isSimulatedCrash(err) {
			removeAllOrLog(attemptDir)
		}
	}()

	if spec.hooks != nil && spec.hooks.BeforeMap != nil {
		if err = recoverHookErr(func() error { return spec.hooks.BeforeMap(ctx, mapID, attempt) }); err != nil {
			return err
		}
	}

	collector := newMapCollector(attemptDir, mapID, spec.Partitions, spec.ShuffleBytes)
	if err = streamInputFile(ctx, spec.InputFiles[mapID], func(recNum int, line string) error {
		return runUserMap(ctx, spec, recNum, line, collector)
	}); err != nil {
		return err
	}

	if spec.hooks != nil && spec.hooks.AfterMap != nil {
		if err = recoverHookErr(func() error { return spec.hooks.AfterMap(ctx, mapID, attempt, collector) }); err != nil {
			return err
		}
	}

	if _, err = collector.flush(); err != nil {
		return err
	}
	if spec.hooks != nil && spec.hooks.CrashMapAttempts != nil &&
		spec.hooks.CrashMapAttempts[AttemptRef{TaskID: mapID, Attempt: attempt}] {
		// 模拟进程在提交标记写出前被杀掉：不发布、不清理。
		return errSimulatedCrash
	}
	marker := filepath.Join(attemptDir, committedMarker)
	if err = writeAtomic(marker, []byte("committed")); err != nil {
		return err
	}
	if err = fsyncDir(taskDir); err != nil {
		return err
	}
	committed = true
	return nil
}

func isSimulatedCrash(err error) bool {
	return err != nil && errors.Is(err, errSimulatedCrash)
}

func runUserMap(ctx context.Context, spec *JobSpec, recNum int, line string, out Emitter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &operatorError{phase: "map", taskID: recNum, panicValue: r}
		}
	}()
	if err = spec.MapFn(ctx, "", line, out); err != nil {
		return fmtWrap(ErrTaskAttempt, "map function: %v", err)
	}
	return nil
}

type operatorError struct {
	phase      string
	taskID     int
	panicValue any
}

func (e *operatorError) Error() string {
	return "mr: operator panic in " + e.phase + ": " + panicString(e.panicValue)
}
func (e *operatorError) Unwrap() error { return ErrOperatorPanic }

func panicString(v any) string {
	switch x := v.(type) {
	case error:
		return x.Error()
	case string:
		return x
	default:
		return "unknown panic"
	}
}

// streamInputFile 按行读取输入文件。每行就是一条记录，键为行内序号（字符串），
// 但默认 Mapper 只使用 value；输入切分完全由文件列表决定，与并发度无关。
func streamInputFile(ctx context.Context, path string, fn func(recNum int, line string) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "open input %s: %v", path, err)
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 64*1024)
	recNum := 0
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		line, rerr := br.ReadString('\n')
		// ReadString 在最后一行无换行时同时返回内容与 io.EOF。
		if len(line) > 0 {
			trimmed := line
			if trimmed[len(trimmed)-1] == '\n' {
				trimmed = trimmed[:len(trimmed)-1]
				if len(trimmed) > 0 && trimmed[len(trimmed)-1] == '\r' {
					trimmed = trimmed[:len(trimmed)-1]
				}
			}
			if err = fn(recNum, trimmed); err != nil {
				return err
			}
			recNum++
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return fmtWrap(ErrTaskAttempt, "read input %s: %v", path, rerr)
		}
	}
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "open dir %s for fsync: %v", dir, err)
	}
	err = d.Sync()
	_ = d.Close()
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "fsync dir %s: %v", dir, err)
	}
	return nil
}
