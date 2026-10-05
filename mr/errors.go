package mr

import (
	"errors"
	"fmt"
)

// 可区分的失败原因。作业定义类错误属于永久错误，不会触发任务重试。
var (
	// ErrInvalidJob 是所有作业定义错误的根。
	ErrInvalidJob = errors.New("mr: invalid job definition")
	// ErrInputNotFound 输入目录不存在或不是目录。
	ErrInputNotFound = fmt.Errorf("%w: input not found", ErrInvalidJob)
	// ErrEmptyInput 输入目录下没有任何可读输入文件。
	ErrEmptyInput = fmt.Errorf("%w: input directory has no files", ErrInvalidJob)
	// ErrInvalidPartitions 分区数小于 1 或超过上限。
	ErrInvalidPartitions = fmt.Errorf("%w: invalid partition count", ErrInvalidJob)
	// ErrMissingOperator 作业没有注册/指定 Mapper 或 Reducer。
	ErrMissingOperator = fmt.Errorf("%w: mapper or reducer missing", ErrInvalidJob)
	// ErrInvalidBudget 内存预算等数值配置非法。
	ErrInvalidBudget = fmt.Errorf("%w: invalid memory budget", ErrInvalidJob)
	// ErrInvalidJobID 作业 ID 为空或含非法字符。
	ErrInvalidJobID = fmt.Errorf("%w: invalid job id", ErrInvalidJob)
	// ErrOutputConflict 输出目录已存在且已提交，但属于另一个作业（指纹不一致）。
	ErrOutputConflict = errors.New("mr: output directory is committed by a different job")
	// ErrUncommittedOutput 输出目录已存在但缺少提交标记，属于未完成的半成品。
	ErrUncommittedOutput = errors.New("mr: output directory contains uncommitted data from an interrupted run")
)

// 任务执行类错误。
var (
	// ErrTaskFailed 是所有任务失败的根，可通过 errors.As 拿到 *TaskError 明细。
	ErrTaskFailed = errors.New("mr: task failed")
	// ErrAttemptsExhausted 任务在最大尝试次数内始终失败。
	ErrAttemptsExhausted = errors.New("mr: task retries exhausted")
	// ErrAborted 作业在执行途中被取消（ctx 取消）。
	ErrAborted = errors.New("mr: job aborted")
)

// FailureKind 描述任务失败的归一化类别。
type FailureKind string

const (
	FailurePanic    FailureKind = "panic"    // 算子 panic
	FailureIO       FailureKind = "io"       // 本地读写错误（可重试）
	FailureCanceled FailureKind = "canceled" // 作业被取消
	FailureUnknown  FailureKind = "unknown"
)

// TaskError 把单个任务的任意失败（panic / IO / 取消）归一成同一类型。
type TaskError struct {
	Phase   string // "map" 或 "reduce"
	TaskID  int    // map 任务为输入文件序号，reduce 任务为分区号
	Attempt int    // 从 1 开始
	Kind    FailureKind
	Cause   error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("%s: %s task %d attempt %d: %v",
		e.Kind, e.Phase, e.TaskID, e.Attempt, e.Cause)
}

func (e *TaskError) Unwrap() error { return e.Cause }

func (e *TaskError) Is(target error) bool { return target == ErrTaskFailed }

// Retriable 判断该失败是否按重试策略重试。panic 与本地 IO 抖动都重试，取消不重试。
func (e *TaskError) Retriable() bool {
	return e.Kind == FailurePanic || e.Kind == FailureIO
}

// AttemptsExhaustedError 表示任务重试用尽。
type AttemptsExhaustedError struct {
	Last *TaskError
}

func (e *AttemptsExhaustedError) Error() string {
	return fmt.Sprintf("%v: %v", ErrAttemptsExhausted, e.Last)
}

func (e *AttemptsExhaustedError) Unwrap() error { return ErrAttemptsExhausted }

// ValidationError 携带可区分的校验失败原因，Cause 一定是上面导出的哨兵错误之一。
type ValidationError struct {
	Reason error
	Detail string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%v: %s", e.Reason, e.Detail)
}

func (e *ValidationError) Unwrap() error { return e.Reason }

func validationError(cause error, format string, args ...any) error {
	return &ValidationError{Reason: cause, Detail: fmt.Sprintf(format, args...)}
}
