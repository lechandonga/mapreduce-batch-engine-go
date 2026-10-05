package mr

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// maxReduceFanIn 限制一次 k 路归并的流数量。当 committed map 输出数超过
// 该值时，reduce 任务先分层归并成中间 spill（每层遵守内存预算的流式处理），
// 再对归并结果分组，保证打开文件数与堆内存始终有界。
const maxReduceFanIn = 8

// committedMapSpills 返回每个 map 任务已提交尝试目录中的 spill 文件。
// 只有带 COMMITTED 标记的目录会被扫描；尝试目录名按 mapID 排序，
// 因此下游归并的来源顺序与并发执行顺序无关。
func committedMapSpills(root string, numMaps int) ([][]string, error) {
	result := make([][]string, numMaps)
	for mapID := 0; mapID < numMaps; mapID++ {
		dir := mapTaskDir(root, mapID)
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmtWrap(ErrTaskAttempt, "read map output dir: %v", err)
		}
		var committed string
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if _, statErr := os.Stat(filepath.Join(dir, e.Name(), committedMarker)); statErr == nil {
				if committed == "" || e.Name() > committed {
					committed = e.Name()
				}
			}
		}
		if committed == "" {
			return nil, fmtWrap(ErrCorruptData, "no committed attempt for map task %d", mapID)
		}
		cdir := filepath.Join(dir, committed)
		spills, err := os.ReadDir(cdir)
		if err != nil {
			return nil, fmtWrap(ErrTaskAttempt, "read committed attempt: %v", err)
		}
		for _, s := range spills {
			name := s.Name()
			if len(name) >= 6 && name[:6] == "spill-" && !s.IsDir() {
				result[mapID] = append(result[mapID], filepath.Join(cdir, name))
			}
		}
		sort.Strings(result[mapID])
	}
	return result, nil
}

// reduceCollector 是 Reducer 的 Emitter。输出先写入 part 临时文件，
// reduce 任务成功时原子发布到 staging 目录；失败时临时文件随尝试目录删除。
type reduceCollector struct {
	f   *os.File
	bw  *bufio.Writer
	dir string
	key string // 输出记录固定写成 key\tvalue\n，保证可读且确定
}

func newReduceCollector(attemptDir, redName string) (*reduceCollector, error) {
	path := filepath.Join(attemptDir, "part.out")
	f, err := os.Create(path)
	if err != nil {
		return nil, fmtWrap(ErrTaskAttempt, "create reduce output: %v", err)
	}
	return &reduceCollector{f: f, bw: bufio.NewWriterSize(f, 64*1024), dir: attemptDir}, nil
}

func (c *reduceCollector) Emit(key, value string) error {
	if _, err := c.bw.WriteString(key); err != nil {
		return fmtWrap(ErrTaskAttempt, "write reduce output: %v", err)
	}
	if err := c.bw.WriteByte('\t'); err != nil {
		return fmtWrap(ErrTaskAttempt, "write reduce output: %v", err)
	}
	if _, err := c.bw.WriteString(value); err != nil {
		return fmtWrap(ErrTaskAttempt, "write reduce output: %v", err)
	}
	return c.bw.WriteByte('\n')
}

func (c *reduceCollector) closeAndPublish(stagingPath string) (err error) {
	if err = c.bw.Flush(); err != nil {
		return fmtWrap(ErrTaskAttempt, "flush reduce output: %v", err)
	}
	if err = c.f.Sync(); err != nil {
		return fmtWrap(ErrTaskAttempt, "sync reduce output: %v", err)
	}
	if err = c.f.Close(); err != nil {
		return fmtWrap(ErrTaskAttempt, "close reduce output: %v", err)
	}
	return publishTempFile(filepath.Join(c.dir, "part.out"), stagingPath)
}

func (c *reduceCollector) abort() {
	_ = c.f.Close()
}

// openPartitionStreams 为一个 reduce 分区打开所有 spill 文件中的对应段。
// 每个段内记录按 (key, seq) 有序；多个段直接组成 k 路归并输入。
func openPartitionStreams(spillSets [][]string, partition, partitions int) ([]*spillStream, func(), error) {
	var streams []*spillStream
	cleanup := func() {
		for _, s := range streams {
			_ = s.reader.Close()
		}
	}
	src := 0
	for _, set := range spillSets {
		for _, p := range set {
			reader, err := openSpillPartition(p, partition, partitions)
			if err != nil {
				cleanup()
				return nil, nil, err
			}
			streams = append(streams, &spillStream{reader: reader, src: src})
			src++
		}
	}
	return streams, cleanup, nil
}

// runReduceAttempt 执行一次 reduce 尝试。
func runReduceAttempt(ctx context.Context, spec *JobSpec, root, staging string, redID, attempt int, spillSets [][]string) (err error) {
	taskDir := reduceTaskDir(root, redID)
	attemptDir := filepath.Join(taskDir, "attempt-"+pad10(attempt))
	if err = os.MkdirAll(attemptDir, 0o755); err != nil {
		return fmtWrap(ErrTaskAttempt, "mkdir reduce attempt: %v", err)
	}
	published := false
	defer func() {
		if !published && !isSimulatedCrash(err) {
			removeAllOrLog(attemptDir)
		}
	}()

	if spec.hooks != nil && spec.hooks.BeforeReduce != nil {
		if err = recoverHookErr(func() error { return spec.hooks.BeforeReduce(ctx, redID, attempt) }); err != nil {
			return err
		}
	}

	streams, cleanup, err := openPartitionStreams(spillSets, redID, spec.Partitions)
	if err != nil {
		return err
	}
	defer cleanup()

	// 流数量过多时分层归并，保证最终归并堆大小不超过 maxReduceFanIn。
	streams, err = coalesceStreams(streams, redID, attemptDir, spec)
	if err != nil {
		return err
	}

	merge, err := newMergingReader(streams)
	if err != nil {
		return err
	}
	defer merge.closeAll()

	collector, err := newReduceCollector(attemptDir, "r-"+pad10(redID))
	if err != nil {
		return err
	}

	var pending codedRecord
	havePending := false
	for {
		var first codedRecord
		if havePending {
			first = pending
			havePending = false
		} else {
			first, err = merge.next()
			if err == io.EOF {
				break
			}
			if err != nil {
				collector.abort()
				return err
			}
		}
		key := first.key
		iter := newGroupIterator(key, first, merge.next)
		if err = runUserReduce(ctx, spec, key, iter, collector); err != nil {
			collector.abort()
			return err
		}
		if iter.hasPending {
			pending = iter.pending
			havePending = true
		}
	}

	if spec.hooks != nil && spec.hooks.AfterReduce != nil {
		if err = recoverHookErr(func() error { return spec.hooks.AfterReduce(ctx, redID, attempt) }); err != nil {
			collector.abort()
			return err
		}
	}

	partName := "part-" + pad10(redID)
	if err = collector.closeAndPublish(filepath.Join(staging, partName)); err != nil {
		return err
	}
	if spec.hooks != nil && spec.hooks.CrashReduceAttempts != nil &&
		spec.hooks.CrashReduceAttempts[AttemptRef{TaskID: redID, Attempt: attempt}] {
		return errSimulatedCrash
	}
	published = true
	return fsyncDir(staging)
}

func runUserReduce(ctx context.Context, spec *JobSpec, key string, values ValueIterator, out Emitter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &operatorError{phase: "reduce", panicValue: r}
		}
	}()
	if err = spec.ReduceFn(ctx, key, values, out); err != nil {
		return fmtWrap(ErrTaskAttempt, "reduce function: %v", err)
	}
	return nil
}

// coalesceStreams 在流数量超过 fan-in 上限时，逐层做流式 k 路归并并落盘。
// 归并过程只保留每条流的堆顶记录，不物化数据，仍然遵守内存上限。
func coalesceStreams(streams []*spillStream, redID int, attemptDir string, spec *JobSpec) ([]*spillStream, error) {
	if len(streams) <= maxReduceFanIn {
		return streams, nil
	}
	round := 0
	for len(streams) > maxReduceFanIn {
		var next []*spillStream
		for start := 0; start < len(streams); start += maxReduceFanIn {
			end := start + maxReduceFanIn
			if end > len(streams) {
				end = len(streams)
			}
			path := filepath.Join(attemptDir, "merge-"+pad10(redID)+"-"+pad10(round)+"-"+pad10(start/maxReduceFanIn))
			merged, err := mergeGroupToSpill(streams[start:end], path, spec.Partitions, redID)
			if err != nil {
				for _, s := range next {
					_ = s.reader.Close()
				}
				return nil, err
			}
			next = append(next, merged)
		}
		for _, s := range streams {
			_ = s.reader.Close()
		}
		streams = next
		round++
	}
	return streams, nil
}

// mergeGroupToSpill 把一组有序流流式归并，仅写出属于本 reduce 分区的记录，
// 生成一个新的单段 spill 供后续归并读取。
func mergeGroupToSpill(group []*spillStream, path string, partitions, redID int) (*spillStream, error) {
	mr, err := newMergingReader(group)
	if err != nil {
		return nil, err
	}
	tmp := path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		_ = mr.closeAll()
		return nil, fmtWrap(ErrTaskAttempt, "create merged spill: %v", err)
	}
	bw := bufio.NewWriterSize(out, 64*1024)
	var recBuf []byte
	writeErr := func(e error) (*spillStream, error) {
		_ = mr.closeAll()
		_ = out.Close()
		_ = os.Remove(tmp)
		return nil, e
	}
	for {
		rec, nerr := mr.next()
		if nerr == io.EOF {
			break
		}
		if nerr != nil {
			return writeErr(nerr)
		}
		recBuf = encodeRecord(recBuf[:0], rec.key, rec.value, rec.seq)
		if _, werr := bw.Write(recBuf); werr != nil {
			return writeErr(fmtWrap(ErrTaskAttempt, "write merged spill: %v", werr))
		}
	}
	if err = bw.Flush(); err != nil {
		return writeErr(err)
	}
	// 生成只包含单个分区段的索引文件，复用 openSpillPartition 读取。
	if err = writeSinglePartitionIndex(out, partitions, redID); err != nil {
		return writeErr(err)
	}
	if err = out.Sync(); err != nil {
		return writeErr(fmtWrap(ErrTaskAttempt, "sync merged spill: %v", err))
	}
	if err = out.Close(); err != nil {
		return writeErr(fmtWrap(ErrTaskAttempt, "close merged spill: %v", err))
	}
	if err = publishTempFile(tmp, path); err != nil {
		return writeErr(err)
	}
	reader, err := openSpillPartition(path, redID, partitions)
	if err != nil {
		return nil, err
	}
	return &spillStream{reader: reader}, nil
}

// writeSinglePartitionIndex 在仅含单一分区数据的文件尾部追加索引：
// 段 0..redID-1 偏移为 0（空段），redID 段偏移为 0，后续段偏移为数据长度。
func writeSinglePartitionIndex(f *os.File, partitions, redID int) error {
	dataEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "seek merged spill: %v", err)
	}
	var b [8]byte
	for p := 0; p < partitions; p++ {
		var off uint64
		switch {
		case p < redID:
			off = 0
		case p == redID:
			off = 0
		default:
			off = uint64(dataEnd)
		}
		binary.BigEndian.PutUint64(b[:], off)
		if _, err = f.Write(b[:]); err != nil {
			return fmtWrap(ErrTaskAttempt, "write merged index: %v", err)
		}
	}
	binary.BigEndian.PutUint64(b[:], uint64(dataEnd))
	if _, err = f.Write(b[:]); err != nil {
		return fmtWrap(ErrTaskAttempt, "write merged index: %v", err)
	}
	return nil
}
