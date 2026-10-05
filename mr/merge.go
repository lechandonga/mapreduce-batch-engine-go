package mr

import (
	"container/heap"
	"io"
)

// spillStream 是归并堆中的一个有序输入流。
type spillStream struct {
	reader *recordReader
	cur    codedRecord
	src    int // 来源序号，仅用于打破同 seq 的平局（正常不会发生）
	valid  bool
}

type mergeHeap struct {
	streams []*spillStream
}

func (h *mergeHeap) Len() int { return len(h.streams) }
func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.streams[i], h.streams[j]
	if a.cur.key != b.cur.key {
		return a.cur.key < b.cur.key
	}
	if a.cur.seq != b.cur.seq {
		return a.cur.seq < b.cur.seq
	}
	return a.src < b.src
}
func (h *mergeHeap) Swap(i, j int) { h.streams[i], h.streams[j] = h.streams[j], h.streams[i] }
func (h *mergeHeap) Push(x any)    { h.streams = append(h.streams, x.(*spillStream)) }
func (h *mergeHeap) Pop() any {
	old := h.streams
	n := len(old)
	item := old[n-1]
	h.streams = old[:n-1]
	return item
}

// mergingReader 对若干个 (key, seq) 有序流做确定性 k 路归并。
// 任何记录都不会丢失或重复：每个流只有在堆顶记录被取走后才推进。
// 堆大小 = 流数量，与数据总量无关，因此即使输入远超内存也不膨胀。
type mergingReader struct {
	h      mergeHeap
	closed []*spillStream
}

func newMergingReader(streams []*spillStream) (*mergingReader, error) {
	m := &mergingReader{}
	for _, s := range streams {
		if err := s.advance(); err != nil {
			_ = m.closeAll()
			return nil, err
		}
		if s.valid {
			m.h.streams = append(m.h.streams, s)
		} else {
			m.closed = append(m.closed, s)
		}
	}
	heap.Init(&m.h)
	if err := m.closeDrained(); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *spillStream) advance() error {
	rec, err := s.reader.Next()
	if err == io.EOF {
		s.valid = false
		return s.reader.Close()
	}
	if err != nil {
		_ = s.reader.Close()
		return err
	}
	s.cur = rec
	s.valid = true
	return nil
}

func (m *mergingReader) closeDrained() error {
	for _, s := range m.closed {
		// 已在 advance 时关闭。
		_ = s
	}
	m.closed = m.closed[:0]
	return nil
}

// next 取下一条全局有序记录；io.EOF 表示全部流耗尽。
func (m *mergingReader) next() (codedRecord, error) {
	if m.h.Len() == 0 {
		return codedRecord{}, io.EOF
	}
	top := m.h.streams[0]
	rec := top.cur
	if err := top.advance(); err != nil {
		return codedRecord{}, err
	}
	if top.valid {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
	return rec, nil
}

func (m *mergingReader) closeAll() error {
	var firstErr error
	for _, s := range m.h.streams {
		if err := s.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.h.streams = nil
	return firstErr
}

// groupIterator 把有序归并流按相同 key 聚合成 ValueIterator。
// 同键的值只在调用 Next 时逐条从流中拉取，不做整体物化，
// 因此某个键即使有海量记录也不会撑爆内存。读到的下一个键的首条记录
// 会缓存在 pending 中交给驱动，绝不丢失。
type groupIterator struct {
	key      string
	first    codedRecord
	hasFirst bool
	// pending / hasPending 在归并流跨到下一个键时填充。
	pending    codedRecord
	hasPending bool
	pull       func() (codedRecord, error)
}

func newGroupIterator(key string, first codedRecord, pull func() (codedRecord, error)) *groupIterator {
	return &groupIterator{key: key, first: first, hasFirst: true, pull: pull}
}

// Next 返回本键的下一个值。遍历正常结束后可读取 pending 得到下一个键。
func (g *groupIterator) Next() (value string, ok bool, err error) {
	if g.hasFirst {
		g.hasFirst = false
		return g.first.value, true, nil
	}
	rec, err := g.pull()
	if err == io.EOF {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if rec.key != g.key {
		g.pending = rec
		g.hasPending = true
		return "", false, nil
	}
	return rec.value, true, nil
}
