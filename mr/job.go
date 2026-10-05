package mr

import (
	"context"
)

// Pair 是 map 输出、shuffle 与 reduce 输入的统一键值对。
// Value 不参与排序、分组与分区，仅作为记录负载随键移动。
type Pair struct {
	Key   string
	Value string
}

// MapContext 提供给 Mapper：Emit 产生一对中间键值。
type MapContext interface {
	Emit(key, value string)
}

// ValueIterator 按确定顺序（来源 map 任务序号、map 内记录序号）
// 枚举同一个键的全部 value，流式迭代，超大分组不会整体进入内存。
type ValueIterator interface {
	Next() (value string, ok bool)
}

// ReduceContext 提供给 Reducer；Emit 写出最终 K/V。
type ReduceContext interface {
	Values() ValueIterator
	Emit(key, value string)
}

// Mapper 处理一条输入记录（一行）。实现必须是并发安全可重入的：
// 多个 map 任务会在不同 goroutine 中调用同一 Mapper。
type Mapper interface {
	Map(key string /*输入文件相对路径*/, value string /*该行内容*/, out MapContext)
}

// Reducer 处理一个分组（一个键的全部记录）。
type Reducer interface {
	Reduce(key string, ctx ReduceContext)
}

// MapperFunc / ReducerFunc 让函数字面量直接满足算子接口。
type MapperFunc func(key, value string, out MapContext)

func (f MapperFunc) Map(key, value string, out MapContext) { f(key, value, out) }

type ReducerFunc func(key string, ctx ReduceContext)

func (f ReducerFunc) Reduce(key string, ctx ReduceContext) { f(key, ctx) }

// JobConfig 描述一次作业的静态定义。
type JobConfig struct {
	// ID 作业标识，用于临时目录命名与日志。允许字母、数字、'-'、'_'。
	ID string
	// InputDir 输入目录，目录下所有常规文件（按相对路径排序）作为输入分片。
	InputDir string
	// OutputDir 最终输出目录。提交采用目录内原子 rename，作业成功后包含
	// _SUCCESS 标记；已提交目录指纹一致时可复用，冲突或缺标记时拒绝。
	OutputDir string

	Mapper  Mapper
	Reducer Reducer

	// ReducePartitions reduce 分区数（>=1）。同一作业固定分区数保证
	// 不同并发度下分区与分组完全一致。
	ReducePartitions int

	// Parallelism 同时执行的任务（map/reduce 各自）并发度，>=1。
	Parallelism int
	// MaxAttempts 单个任务最大尝试次数（含首次），>=1。
	MaxAttempts int

	// MapMemoryBytes / ReduceMemoryBytes 是各阶段单任务的内存预算（字节）。
	// Map：缓冲区中 Pair 的估算占用达到预算即整体排序落盘。
	// Reduce：每个 run 读取器、归并堆与输出缓冲均在固定预算内流式处理。
	MapMemoryBytes    int
	ReduceMemoryBytes int

	// TempRoot 中间文件根目录（默认 系统临时目录/mr-<jobid>）。
	// 无论作业成功还是失败，引擎结束时都会清理本次作业的临时目录。
	TempRoot string

	// OverwriteUncommitted 为 true 时，输出目录存在半成品（无 _SUCCESS）
	// 会先清空再执行；默认 false，即明确拒绝并要求人工确认。
	OverwriteUncommitted bool

	// FaultHook 在每个任务尝试开始时回调，返回 error 会注入一次失败，
	// 用于测试“临时读写错误/算子 panic/中途退出”等故障。生产代码留空。
	FaultHook FaultHook
}

// FaultHookKind 指定注入的故障类型。
type FaultHookKind string

const (
	FaultNone  FaultHookKind = ""
	FaultPanic FaultHookKind = "panic"
	FaultIO    FaultHookKind = "io"
	FaultExit  FaultHookKind = "exit" // 模拟执行中途退出：清空任务工作目录后返回错误
)

// FaultHook 报告某次尝试；返回非 FaultNone 时引擎按该类型制造失败。
type FaultHook func(phase string, taskID, attempt int) FaultHookKind

// Job 是校验通过后的作业句柄，由 NewJob 构造。
type Job struct {
	cfg         JobConfig
	fingerprint string   // 输入与算子结构指纹，用于输出复用/冲突判定
	inputFiles  []string // 相对路径，字典序，即 map 任务序号
}

// Stats 汇总一次执行的计数器，便于测试与日志判定。
type Stats struct {
	MapTasks        int
	ReduceTasks     int
	MapAttempts     int
	ReduceAttempts  int
	MapSpillFiles   int
	ReduceRuns      int64 // reduce 侧归并的 run 总数（所有分区之和）
	InputRecords    int64
	MapOutputPairs  int64
	OutputRecords   int64
	ReusedCommitted bool
}

// Result 是 Run 的返回值。
type Result struct {
	OutputDir string
	Stats     Stats
}

// Runner 执行作业。零值不可用，必须由 NewEngine 创建。
type Runner struct {
	engine *Engine
	job    *Job
}

// Run 执行（或在输出已提交且指纹一致时复用）作业，直到完成、取消或失败。
func (r *Runner) Run(ctx context.Context) (*Result, error) {
	return r.engine.run(ctx, r.job)
}
