# 本地 MapReduce 批处理执行引擎（Go）

纯本地文件、无外部服务/框架依赖的 MapReduce 执行引擎：输入来自本地目录，
Shuffle 中间数据走本地临时文件，最终输出原子提交到本地目录。

设计目标：

- **确定性**：同一份输入在任意并发度、任意失败重试下，分区、分组与输出完全一致；
  同一个键的所有记录一定进入同一个 Reduce 分组。
- **有界内存**：Map/Reduce 各有显式字节预算；超预算按确定策略落盘，键倾斜、
  数据量远超内存的作业也能在预算内跑完。
- **可恢复/幂等**：任务失败归一分类、按策略重试；多个尝试并发只有一份结果提交；
  中断后重跑要么安全复用已提交结果，要么明确拒绝（也可显式覆盖），绝不混合半成品。
- **不留垃圾**：失败尝试、失败/成功作业的中间文件都会被清理，不会被下次运行误读。

## 作业定义与提交

```go
import "github.com/lechandonga/mapreduce-batch-engine-go/mr"

cfg := mr.JobConfig{
    ID:                  "wordcount",
    InputDir:            "data/in",
    OutputDir:           "data/out",
    Mapper:              myMapper,   // mr.Mapper 接口或 mr.MapperFunc
    Reducer:             myReducer,  // mr.Reducer 接口或 mr.ReducerFunc
    ReducePartitions:    4,
    Parallelism:         2,          // 每阶段同时执行的任务数
    MaxAttempts:         3,          // 单任务最大尝试次数（含首次）
    MapMemoryBytes:      1 << 20,    // map 单任务内存预算
    ReduceMemoryBytes:   1 << 20,    // reduce 单任务内存预算
    TempRoot:            "",         // 默认 $TMPDIR/mr-jobs
    OverwriteUncommitted: false,     // true=清空半成品输出后重跑
}

runner, err := mr.NewEngine().Submit(cfg) // 写任何数据之前完成全部校验
res, err := runner.Run(ctx)
```

也可以通过注册中心按名字提交算子：

```go
reg := mr.NewRegistry()
reg.RegisterMapper("wordcount", mr.MapperFunc(...))
reg.RegisterReducer("sum", mr.ReducerFunc(...))
cfg, err = reg.WithOperators(cfg, "wordcount", "sum")
```

算子接口：

```go
type Mapper interface  { Map(inputFileRelPath, line string, out MapContext) }
type Reducer interface { Reduce(key string, ctx ReduceContext) }

type ValueIterator interface { Next() (value string, ok bool) } // 流式，不整体加载分组
```

输入是 `InputDir` 下全部常规文件（递归、相对路径字典序），每个文件是一个 Map 分片；
文件按行读取（单行上限 64MiB）。

## 校验（在写出任何数据之前）

`Submit` 会拒绝并返回可区分的哨兵错误（`errors.Is` 判定）：

| 错误 | 触发条件 |
| --- | --- |
| `mr.ErrInputNotFound` | 输入目录不存在/不可读 |
| `mr.ErrEmptyInput` | 输入目录没有常规文件 |
| `mr.ErrInvalidPartitions` | 分区数不在 `[1, 100000]` |
| `mr.ErrInvalidBudget` | 内存预算小于 64KiB |
| `mr.ErrMissingOperator` | Mapper/Reducer 为空，或注册名不存在/重复 |
| `mr.ErrInvalidJobID` | Job ID 为空或含 `[A-Za-z0-9_-]` 之外字符 |
| `mr.ErrUncommittedOutput` | 输出目录非空且没有 `_SUCCESS`（别的运行的半成品） |
| `mr.ErrOutputConflict` | 输出已提交，但输入/算子指纹与本次作业不同 |

## 分区与 Shuffle 文件布局

分区函数只依赖 `(key, partitions)`：`FNV-1a(key) mod partitions`
（空键固定 0 区）。它不依赖并发、goroutine 调度或 map 遍历顺序，
所以同一键的所有记录必然落入同一分区、同一分组，且不同并发度结果一致。

```
$TMPDIR/mr-jobs/<jobID>/run-<随机后缀>/
  map/attempt-<尝试号>/map-<分片号>/
      spill-<run序号>-part-<分区号>.bin
  reduce/attempt-<尝试号>/
      merged-r<轮>-p<分区>-<片>.bin      # 仅当需要分段归并
<输出目录>/
  .staging-<随机后缀>/.part-r-<分区>.attempt-<尝试号>.tmp
  part-r-00000 …                        # 提交后的结果
  _SUCCESS                              # JSON 提交标记（含输入指纹与统计）
```

spill/run 二进制帧格式（小端、CRC32 校验）：

```
bodyLen u32 | keyLen u32 | key | valueLen u32 | value | sourceMap i32 | seq i64 | crc32 u32
```

`sourceMap`（输入分片序号）与 `seq`（map 内发射序号）是 tie-break：
归并顺序固定为 `key ↑, sourceMap ↑, seq ↑`。坏帧/截断帧会报 IO 错误，
不会静默丢/重记录。

## 内存预算与落盘策略

- **Map**：每条 Pair 按 `96B + len(key) + len(value)` 估算。缓冲区估算占用达到
  `MapMemoryBytes` 时，整批按 `(partition,key,sourceMap,seq)` 排序后按分区写成一个
  spill run，然后清空复用。驻留内存上界 ≈ 预算 + 单条记录（单条 ≤64MiB 硬上限）。
  因此单键记录再多也只是让该分区的 run 变长，不会撑爆进程。
- **Reduce**：每个 run 只用一个 64KiB 读缓冲 + 当前一帧，做 k 路最小堆归并；
  分组通过 `ValueIterator` 流式喂给 Reducer，输出带 64KiB 写缓冲。
  同时打开的 run 数封顶 `maxMergeFanIn=32`；run 更多时先做多轮分段归并
  （每轮把 32 个 run 合成 1 个），再进入最终归并。

## 失败重试与错误归一

算子 `panic`、本地读写错误、执行中途退出都会被捕获成 `*mr.TaskError`：

```go
type TaskError struct {
    Phase string; TaskID, Attempt int
    Kind  FailureKind // FailurePanic | FailureIO | FailureCanceled | FailureUnknown
    Cause error
}
```

- `FailurePanic` / `FailureIO`：可重试，最多 `MaxAttempts` 次，用尽后返回
  `ErrAttemptsExhausted`（可用 `errors.As` 取出最后一次 `TaskError`）。
- `FailureCanceled`（ctx 取消）：不重试，作业返回 `mr.ErrAborted`。
- 每次尝试使用独立目录，失败即整体删除；reducer 的临时输出写在私有
  `.staging-*` 里，下游与最终目录在提交前完全看不到。

测试可通过 `JobConfig.FaultHook` 注入 `FaultPanic` / `FaultIO` / `FaultExit`。

## 输出提交与幂等语义

1. Reduce 结果先写 `输出目录/.staging-<rand>/.part-r-*.tmp` 并 `fsync`。
2. 提交时逐分区用**硬链接**（同文件系统、目标已存在即失败）把结果链成
   `part-r-*`，所以多个尝试并发时只有一个能成功，另一个得到 `ErrOutputConflict`，
   记录数与输出文件不会翻倍。
3. 最后以 `O_EXCL` 写 `_SUCCESS`（JSON，含输入内容 SHA-256 指纹、分区数、
   算子类型与统计），再 fsync 目录。
4. 重跑同一输出位置：
   - 有 `_SUCCESS` 且指纹一致 → 直接复用，不重算（`Stats.ReusedCommitted=true`）；
   - 有 `_SUCCESS` 但指纹不一致 → `ErrOutputConflict` 拒绝；
   - 没有 `_SUCCESS` 的非空目录 → `ErrUncommittedOutput` 拒绝；
     显式 `OverwriteUncommitted=true` 时先清空再重跑。
5. 作业结束（成功/失败/取消）删除本次 `run-*` 与 `.staging-*`；启动时还会清理
   本 jobID 下遗留的历史 `run-*`（进程被杀/崩溃后残留）。

## 本地验证

```bash
# 普通测试
go test ./...

# 竞态检测（需要 CGO）
CGO_ENABLED=1 go test -race -count=1 ./...

# 只跑场景测试并查看覆盖了哪些场景/输入/判定
go test ./mr/ -v 2>&1 | grep "scenario="

# 端到端示例
go run ./cmd/wordcount -in testdata/input -out /tmp/wc-out \
  -partitions 4 -parallelism 3 -map-budget 1048576 -reduce-budget 1048576
```

测试场景一览（均在 `-race` 下通过）：

- `TestSmokeWordCount`：词频统计正常作业；
- `TestValidationErrors`：各类非法作业定义的可区分错误；
- `TestIdempotentReuseAndConflict`：复用、指纹冲突、半成品拒绝；
- `TestKeySkewAndSpillUnderBudget`：键倾斜 + 极小预算下多次落盘；
- `TestLargeJobRunsBeyondMemory`：数据量远超内存，触发分段归并；
- `TestDeterminismAcrossParallelism`：并发度 1/2/5/9 输出逐行一致、同键同组；
- `TestFailureRetries`：panic / 临时 IO / 中途退出注入后重试成功；
- `TestRetriesExhausted`：重试用尽的错误归一；
- `TestNoLeftoverTempFiles`：结束后无中间文件残留；
- `TestConcurrentSubmissionsSingleCommit`：3 个尝试并发仅一份提交；
- `TestOverwriteUncommittedRerun`：中断后先拒绝、再显式覆盖重跑；
- `TestMemoryBoundUnderSkew`：12 万行倾斜数据在 64KiB 任务预算下的堆峰值有界。
