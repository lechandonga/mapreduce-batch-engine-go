package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// 每条 Pair 的固定估算开销（切片槽、字符串头、结构体填充之外的保守值）。
const pairFixedOverhead = 96

// rec 是 shuffle 与归并全程使用的记录形态。
type rec struct {
	key       string
	value     string
	sourceMap int32
	seq       int64
	size      int
}

func estimateSize(key, value string) int {
	return pairFixedOverhead + len(key) + len(value)
}

// spillBuffer 是 map 任务的内存缓冲区：达到预算即整体排序、按分区落盘，
// 之后清空复用。任一时刻驻留内存的 Pair 总估算值不超过预算 + 单条记录大小。
type spillBuffer struct {
	budget int
	buf    []rec
	bytes  int
}

func newSpillBuffer(budget int) *spillBuffer {
	return &spillBuffer{budget: budget, buf: make([]rec, 0, 1024)}
}

// Add 追加记录；返回 true 表示调用方应在加入前先 Spill。
func (b *spillBuffer) Add(r rec) bool {
	if b.bytes > 0 && b.bytes+r.size > b.budget {
		return true
	}
	b.buf = append(b.buf, r)
	b.bytes += r.size
	return false
}

func (b *spillBuffer) Len() int { return len(b.buf) }

// Spill 排序并按分区写出，返回每个非空分区生成的文件路径。
// 排序键为 (partition, key, sourceMap, seq)，保证：
//   - 同键记录连续且在同一分区文件中；
//   - 同一 run 内键有序，供 reduce 做 k 路归并；
//   - tie-break 完全确定，与并发度无关。
func (b *spillBuffer) Spill(dir string, runSeq, partitions int) ([]string, error) {
	sort.SliceStable(b.buf, func(i, j int) bool {
		a, c := b.buf[i], b.buf[j]
		if a.partitionForShuffle(partitions) != c.partitionForShuffle(partitions) {
			return a.partitionForShuffle(partitions) < c.partitionForShuffle(partitions)
		}
		if a.key != c.key {
			return a.key < c.key
		}
		if a.sourceMap != c.sourceMap {
			return a.sourceMap < c.sourceMap
		}
		return a.seq < c.seq
	})

	var paths []string
	var w *bufio.Writer
	var f *os.File
	curPart := -1
	closeCurrent := func() error {
		if f == nil {
			return nil
		}
		if err := w.Flush(); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		err := f.Close()
		f, w = nil, nil
		return err
	}

	for _, r := range b.buf {
		p := partitionFor(r.key, partitions)
		if p != curPart {
			if err := closeCurrent(); err != nil {
				return nil, err
			}
			path := filepath.Join(dir, fmt.Sprintf("spill-%06d-part-%05d.bin", runSeq, p))
			nf, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return nil, err
			}
			f = nf
			w = bufio.NewWriterSize(f, 64*1024)
			curPart = p
			paths = append(paths, path)
		}
		if err := writeFrame(w, r.key, r.value, r.sourceMap, r.seq); err != nil {
			closeCurrent()
			return nil, err
		}
	}
	if err := closeCurrent(); err != nil {
		return nil, err
	}

	b.buf = b.buf[:0]
	b.bytes = 0
	return paths, nil
}

func (r rec) partitionForShuffle(partitions int) int {
	return partitionFor(r.key, partitions)
}

// runReader 顺序读取一个落盘 run，带 64KB 读缓冲，每次只驻留一帧。
type runReader struct {
	f   *os.File
	br  *bufio.Reader
	cur rec
	err error
}

func openRun(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &runReader{f: f, br: bufio.NewReaderSize(f, 64*1024)}, nil
}

// advance 读到下一帧；EOF 时返回 (false, nil)。
func (r *runReader) advance() bool {
	h, err := readFrame(r.br)
	if err == io.EOF {
		r.err = nil
		return false
	}
	if err != nil {
		r.err = err
		return false
	}
	r.cur = rec{key: h.key, value: h.value, sourceMap: h.sourceMap, seq: h.seq,
		size: estimateSize(h.key, h.value)}
	return true
}

func (r *runReader) Close() error { return r.f.Close() }

// mergeRuns 对若干已按键排序的 run 做 k 路归并，返回流式迭代器。
// 每个 run 仅占用一个读缓冲，调用方负责 Close。
func mergeRuns(readers []*runReader) (*mergeIterator, error) {
	mi := &mergeIterator{readers: readers}
	mi.heap = make(heapEntries, 0, len(readers))
	for i, rd := range readers {
		if rd.advance() {
			mi.heap = append(mi.heap, heapEntry{rd: rd, idx: i})
		} else if rd.err != nil {
			mi.Close()
			return nil, rd.err
		}
	}
	mi.init()
	return mi, nil
}

type heapEntry struct {
	rd  *runReader
	idx int
}

// mergeIterator 输出顺序：key 升序，同键按 (sourceMap, seq) 升序。
type mergeIterator struct {
	readers []*runReader
	heap    heapEntries
	pushed  *rec // 至多一个回放记录（分组切换时 peek 用）
}

func (m *mergeIterator) init() {
	// 堆规模在 reduce 预算内受控（见 engine 的分段归并），直接堆化。
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.down(i)
	}
}

func (m *mergeIterator) less(i, j int) bool {
	a, b := m.heap[i].rd.cur, m.heap[j].rd.cur
	if a.key != b.key {
		return a.key < b.key
	}
	if a.sourceMap != b.sourceMap {
		return a.sourceMap < b.sourceMap
	}
	return a.seq < b.seq
}

func (m *mergeIterator) swap(i, j int) { m.heap[i], m.heap[j] = m.heap[j], m.heap[i] }

func (m *mergeIterator) down(i int) {
	for {
		l := 2*i + 1
		if l >= len(m.heap) {
			return
		}
		j := l
		if r := l + 1; r < len(m.heap) && m.less(r, l) {
			j = r
		}
		if !m.less(j, i) {
			return
		}
		m.swap(i, j)
		i = j
	}
}

func (m *mergeIterator) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !m.less(i, p) {
			return
		}
		m.swap(i, p)
		i = p
	}
}

// Next 弹出全局最小记录并回填来源 run。
func (m *mergeIterator) Next() (rec, bool, error) {
	if m.pushed != nil {
		r := *m.pushed
		m.pushed = nil
		return r, true, nil
	}
	if len(m.heap) == 0 {
		return rec{}, false, nil
	}
	top := m.heap[0]
	out := top.rd.cur
	if top.rd.advance() {
		m.down(0)
	} else if top.rd.err != nil {
		err := top.rd.err
		m.heap[0] = m.heap[len(m.heap)-1]
		m.heap = m.heap[:len(m.heap)-1]
		if len(m.heap) > 0 {
			m.down(0)
		}
		return rec{}, false, err
	} else {
		m.heap[0] = m.heap[len(m.heap)-1]
		m.heap = m.heap[:len(m.heap)-1]
		if len(m.heap) > 0 {
			m.down(0)
		}
	}
	return out, true, nil
}

// PeekKey 返回当前最小键（堆顶），不消费。
func (m *mergeIterator) PeekKey() string {
	if m.pushed != nil {
		return m.pushed.key
	}
	if len(m.heap) == 0 {
		return ""
	}
	return m.heap[0].rd.cur.key
}

// pushBack 回放恰好一条记录，供分组迭代器在跨界时归还首条下一组记录。
func (m *mergeIterator) pushBack(r rec) { m.pushed = &r }

// Close 关闭所有仍打开的 run，返回第一个关闭错误。
func (m *mergeIterator) Close() error {
	var first error
	for _, rd := range m.readers {
		if rd != nil {
			if err := rd.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

type heapEntries []heapEntry

// mergeRunFiles 把一组 run 文件归并后写成新的 run 文件（分段归并用）。
// sourceMap/seq 原样保留，因此最终分组顺序的确定性不被破坏。
func mergeRunFiles(inputs []string, outPath string) error {
	readers := make([]*runReader, 0, len(inputs))
	for _, p := range inputs {
		rd, err := openRun(p)
		if err != nil {
			return err
		}
		readers = append(readers, rd)
	}
	mi, err := mergeRuns(readers)
	if err != nil {
		return err
	}
	defer mi.Close()

	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 64*1024)
	for {
		r, ok, err := mi.Next()
		if err != nil {
			f.Close()
			return err
		}
		if !ok {
			break
		}
		if err := writeFrame(bw, r.key, r.value, r.sourceMap, r.seq); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
