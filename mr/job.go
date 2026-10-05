package mr

import "context"

// Record 是引擎内部统一的键值记录。键决定分区与分组，值由算子解释。
type Record struct {
	Key   string
	Value string
}

// Mapper 处理一条输入记录，通过 Emitter 输出零到多条记录。
// 实现必须是并发安全可重入的：同一个 Mapper 会被多个 worker 并发调用。
type Mapper func(ctx context.Context, key, value string, emit Emitter) error

// Reducer 处理一个键的全部分组。values 只能在本次调用内消费，
// 其数据由内存与磁盘共同支撑，单键记录再多也不会整体驻留内存。
// Reducer 同样会被多个 worker 并发调用。
type Reducer func(ctx context.Context, key string, values ValueIterator, emit Emitter) error

// Emitter 接收算子输出。Emitter 在算子返回后不得再被使用。
type Emitter interface {
	Emit(key, value string) error
}

// ValueIterator 顺序遍历某个键的全部值。遍历顺序是确定的：
// 先按 map 任务序号（由输入文件排序得到）、再按 map 输出顺序排列。
type ValueIterator interface {
	Next() (value string, ok bool, err error)
}

// JobSpec 是作业的完整定义。所有字段在提交时统一校验，
// 任何非法配置都会在引擎写出任何数据之前被拒绝。
type JobSpec struct {
	// Name 仅用于日志与错误信息。
	Name string

	// InputFiles 输入文件列表（必须是普通文件、必须存在）。
	// 文件会按路径排序后编号，map 任务的划分与并发度无关。
	InputFiles []string

	// OutputDir 最终输出目录。不存在时创建；已存在时只有两种合法情况：
	// 1) 内含同一输入的已提交结果（直接复用）；
	// 2) 为空目录或只包含引擎可识别的上一次未提交残留（清理后重跑）。
	// 其他内容一律以 ErrOutputConflict 拒绝。
	OutputDir string

	// Partitions Reduce 分区数（即 Reduce 任务数），必须 >= 1。
	Partitions int

	// Workers 同时执行任务的 worker 数，必须 >= 1。
	Workers int

	// MaxAttempts 单个任务的最大尝试次数（含首次），必须 >= 1。
	MaxAttempts int

	// ShuffleBytes 单个 shuffle 收集器（map 侧或 reduce 侧）可用于
	// 缓冲记录序列化字节数的内存预算，必须 > 0。超出预算时按确定策略
	// 落盘，进程内存上限 = Workers * ShuffleBytes（再加固定小开销）。
	ShuffleBytes int

	MapFn    Mapper
	ReduceFn Reducer

	// hooks 仅供测试使用：在任务生命周期注入失败。
	hooks *Hooks
}

// RunResult 描述一次 Run 的结果。
type RunResult struct {
	// Reused 为 true 表示没有重新计算，直接复用了已提交的输出。
	Reused bool
	// MapTasks / ReduceTasks 实际执行（非复用）的任务数。
	MapTasks    int
	ReduceTasks int
	// MapAttempts / ReduceAttempts 各阶段的尝试总数（含重试）。
	MapAttempts    int
	ReduceAttempts int
}

func (s *JobSpec) validate() error {
	if s == nil {
		return jobError("validate", fmtWrap(ErrInvalidJob, "nil job spec"))
	}
	if s.MapFn == nil || s.ReduceFn == nil {
		return jobError("validate", fmtWrap(ErrInvalidJob, "MapFn and ReduceFn must both be registered"))
	}
	if s.Partitions < 1 {
		return jobError("validate", fmtWrap(ErrInvalidJob, "Partitions must be >= 1, got %d", s.Partitions))
	}
	if s.Workers < 1 {
		return jobError("validate", fmtWrap(ErrInvalidJob, "Workers must be >= 1, got %d", s.Workers))
	}
	if s.MaxAttempts < 1 {
		return jobError("validate", fmtWrap(ErrInvalidJob, "MaxAttempts must be >= 1, got %d", s.MaxAttempts))
	}
	if s.ShuffleBytes < minShuffleBytes {
		return jobError("validate", fmtWrap(ErrInvalidJob, "ShuffleBytes must be >= %d, got %d", minShuffleBytes, s.ShuffleBytes))
	}
	if len(s.InputFiles) == 0 {
		return jobError("validate", fmtWrap(ErrInvalidJob, "at least one input file is required"))
	}
	if s.OutputDir == "" {
		return jobError("validate", fmtWrap(ErrInvalidJob, "OutputDir is required"))
	}
	return nil
}

const minShuffleBytes = 64
