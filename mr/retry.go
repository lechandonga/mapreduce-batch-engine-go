package mr

import (
	"context"
	"errors"
	"fmt"
)

// runTaskWithRetries 按 MaxAttempts 执行一个 map 任务。
// 每次尝试使用全新的 attempt-N 目录；成功尝试保留并带 COMMITTED，
// 失败尝试的目录被删除（模拟崩溃除外，此时保留无 COMMITTED 的残留，
// 因为它不会被任何下游读取，最终由 scratch 清理回收）。
func runTaskWithRetries(ctx context.Context, spec *JobSpec, scratch string, _ bool, taskID int) (attempts int, err error) {
	for attempt := 0; attempt < spec.MaxAttempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return attempt + 1, err
		}
		aErr := runMapAttempt(ctx, spec, scratch, taskID, attempt)
		attempts++
		if aErr == nil {
			return attempts, nil
		}
		err = aErr
	}
	return attempts, fmt.Errorf("%w: map task %d failed after %d attempts: %w",
		ErrAttemptBudgetExhausted, taskID, attempts, err)
}

func runTaskWithRetriesReduce(ctx context.Context, spec *JobSpec, scratch, staging string, taskID int, spillSets [][]string) (attempts int, err error) {
	for attempt := 0; attempt < spec.MaxAttempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return attempt + 1, err
		}
		aErr := runReduceAttempt(ctx, spec, scratch, staging, taskID, attempt, spillSets)
		attempts++
		if aErr == nil {
			return attempts, nil
		}
		err = aErr
	}
	return attempts, fmt.Errorf("%w: reduce task %d failed after %d attempts: %w",
		ErrAttemptBudgetExhausted, taskID, attempts, err)
}

var _ = errors.Is
