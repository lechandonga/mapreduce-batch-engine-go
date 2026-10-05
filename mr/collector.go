package mr

import (
	"os"
	"path/filepath"
	"sync/atomic"
)

// seqBits 给每个 map 任务预留的本地顺序号空间。
// 全局顺序 = mapTaskID << seqBits | localSeq，
// 因此归并顺序先按 map 任务（输入排序决定）、再按输出顺序，完全确定。
const seqBits = 40
const localSeqMask = uint64(1)<<seqBits - 1

func compositeSeq(mapID, local uint64) (uint64, error) {
	if local > localSeqMask {
		return 0, fmtWrap(ErrInvalidJob, "single map task emitted more than 2^%d records", seqBits)
	}
	return uint64(mapID)<<seqBits | local, nil
}

// mapCollector 是 map 任务的 Emitter。记录先攒在内存里，序列化字节数
// 达到 ShuffleBytes 预算时立即排序并落盘成一个 spill 文件；
// 因此驻留内存的记录总量始终 <= 预算 + 单条最大记录。
type mapCollector struct {
	dir        string // 当前尝试目录
	mapID      int
	partitions int
	budget     int
	mem        []codedRecord
	bytes      int
	spillNum   int
	localSeq   uint64
}

func newMapCollector(dir string, mapID, partitions, budget int) *mapCollector {
	return &mapCollector{dir: dir, mapID: mapID, partitions: partitions, budget: budget}
}

func (c *mapCollector) Emit(key, value string) error {
	if c.localSeq > localSeqMask {
		return fmtWrap(ErrInvalidJob, "single map task emitted more than 2^%d records", seqBits)
	}
	seq, err := compositeSeq(uint64(c.mapID), c.localSeq)
	if err != nil {
		return err
	}
	c.localSeq++
	size := encodedLen(key, value)
	// 超预算：先把已有缓冲落盘，再决定这条记录是进新缓冲还是单独成 spill。
	if c.bytes+size > c.budget && len(c.mem) > 0 {
		if err := c.spill(); err != nil {
			return err
		}
	}
	c.mem = append(c.mem, codedRecord{key: key, value: value, seq: seq})
	c.bytes += size
	// 单条记录本身就超过预算：立刻独自落盘，避免它长期占据内存，
	// 内存峰值上界因此是“预算 + 单条最大记录”，而不是记录数无界增长。
	if c.bytes >= c.budget {
		if err := c.spill(); err != nil {
			return err
		}
	}
	return nil
}

func (c *mapCollector) spill() error {
	sortRecords(c.mem)
	name := spillFileName(c.spillNum)
	c.spillNum++
	path := filepath.Join(c.dir, name)
	if err := writeSpillFile(path, c.mem, c.partitions); err != nil {
		return err
	}
	c.mem = nil
	c.bytes = 0
	return nil
}

// flush 把剩余内存记录写成最后一个 spill（即使为空也不写文件），
// 返回已发布的 spill 文件数。
func (c *mapCollector) flush() (int, error) {
	if len(c.mem) > 0 {
		if err := c.spill(); err != nil {
			return 0, err
		}
	}
	return c.spillNum, nil
}

func spillFileName(n int) string {
	return "spill-" + pad10(n)
}

func pad10(n int) string {
	const digits = "0123456789"
	var b [10]byte
	for i := len(b) - 1; i >= 0; i-- {
		b[i] = digits[n%10]
		n /= 10
	}
	return string(b[:])
}

// attemptCleaner 保证失败尝试的目录一定被移除，绝不留下半成品。
func removeAllOrLog(path string) {
	_ = os.RemoveAll(path)
}

// attemptCounter 生成进程内单调递增的尝试编号，用于目录命名。
type attemptCounter struct{ n int64 }

func (a *attemptCounter) next() int { return int(atomic.AddInt64(&a.n, 1) - 1) }
