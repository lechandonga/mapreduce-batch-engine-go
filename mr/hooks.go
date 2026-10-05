package mr

import "context"

// Hooks 用于在任务生命周期注入故障（主要服务于测试）。
// 返回错误时该次尝试失败，引擎按正常重试策略处理。
type Hooks struct {
	BeforeMap func(ctx context.Context, mapID, attempt int) error
	AfterMap  func(ctx context.Context, mapID, attempt int, collector MapCollectorView) error

	BeforeReduce func(ctx context.Context, redID, attempt int) error
	AfterReduce  func(ctx context.Context, redID, attempt int) error

	// CrashMapAttempts / CrashReduceAttempts 中的 (taskID, attempt) 对，
	// 在对应尝试即将提交时模拟进程被 SIGKILL：错误返回且不清理尝试目录，
	// 用来验证未完成尝试的残留文件对下游与下次运行都不可见。
	CrashMapAttempts    map[AttemptRef]bool
	CrashReduceAttempts map[AttemptRef]bool
}

// AttemptRef 标识“第几个任务的第几次尝试”（attempt 从 0 开始）。
type AttemptRef struct {
	TaskID  int
	Attempt int
}

// MapCollectorView 暴露给 AfterMap 钩子的只读信息。
type MapCollectorView interface {
	Emitted() int
	Spills() int
}

func (c *mapCollector) Emitted() int { return int(c.localSeq) }
func (c *mapCollector) Spills() int  { return c.spillNum }

// errSimulatedCrash 表示“进程被杀死”式失败：尝试目录保留不清理。
var errSimulatedCrash = fmtWrap(ErrTaskAttempt, "simulated crash before commit")
