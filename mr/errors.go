package mr

import "fmt"

// 错误类型用于区分作业失败的原因。所有引擎返回的错误都会包装这些哨兵错误，
// 调用方可以使用 errors.Is 进行判别。
var (
	// ErrInvalidJob 作业定义非法（分区数、内存预算、输入输出配置等）。
	ErrInvalidJob = fmt.Errorf("mr: invalid job definition")
	// ErrInputNotFound 输入文件或目录不存在。
	ErrInputNotFound = fmt.Errorf("mr: input not found")
	// ErrOutputConflict 输出位置已被占用，且无法安全复用。
	ErrOutputConflict = fmt.Errorf("mr: output location conflict")
	// ErrConcurrentRun 同一个输出位置已有另一个作业实例在运行。
	ErrConcurrentRun = fmt.Errorf("mr: another run is using the output location")
	// ErrFingerprintMismatch 已提交结果对应的输入与本次输入不一致。
	ErrFingerprintMismatch = fmt.Errorf("mr: committed output was produced from different input")
	// ErrTaskAttempt 单个任务尝试失败（读写错误等）。
	ErrTaskAttempt = fmt.Errorf("mr: task attempt failed")
	// ErrOperatorPanic 用户注册的 Map/Reduce 函数发生 panic。
	ErrOperatorPanic = fmt.Errorf("mr: operator panic")
	// ErrCorruptData 读取到损坏或不完整的中间记录。
	ErrCorruptData = fmt.Errorf("mr: corrupt shuffle data")
	// ErrAttemptBudgetExhausted 任务重试次数耗尽。
	ErrAttemptBudgetExhausted = fmt.Errorf("mr: task retries exhausted")
)

// JobError 携带作业级别的上下文信息。
type JobError struct {
	Op  string
	Err error
}

func (e *JobError) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *JobError) Unwrap() error { return e.Err }

func jobError(op string, err error) error { return &JobError{Op: op, Err: err} }
