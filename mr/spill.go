package mr

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// spill 文件中的记录由 recordReader 顺序读出。
// 读到截断或长度非法的数据时返回 ErrCorruptData，避免半成品被静默接受。

// spill 文件布局（一个 spill = 一次内存消化的产物）：
//
//	[ 分区0 的有序记录 ][ 分区1 的有序记录 ]...[ 分区P-1 的记录 ]
//	[ P 个 uint64 段起始偏移 ][ uint64 索引自身的起始偏移 ]
//
// 每个段内按 (key, seq) 排序；由于全局排序也是 (key, seq)，
// 按分区切分后各段内部顺序保持不变。尾部索引让 reducer
// 直接 seek 到自己分区的段，无需扫描他人数据。
func writeSpillFile(path string, recs []codedRecord, partitions int) (err error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "create spill %s: %v", path, err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	bw := bufio.NewWriterSize(f, 64*1024)
	var buf []byte
	var indexBytes [8]byte
	index := make([]uint64, 0, partitions)
	current := -1
	flushOffset := func(partition int) error {
		off, err2 := f.Seek(0, io.SeekCurrent)
		if err2 != nil {
			return fmtWrap(ErrTaskAttempt, "seek spill %s: %v", path, err2)
		}
		if err2 = bw.Flush(); err2 != nil {
			return fmtWrap(ErrTaskAttempt, "flush spill %s: %v", path, err2)
		}
		_ = partition
		index = append(index, uint64(off))
		return nil
	}
	for i := range recs {
		p := partitionForKey(recs[i].key, partitions)
		for current < p {
			current++
			if err = flushOffset(current); err != nil {
				return err
			}
		}
		buf = encodeRecord(buf[:0], recs[i].key, recs[i].value, recs[i].seq)
		if _, err = bw.Write(buf); err != nil {
			return fmtWrap(ErrTaskAttempt, "write spill %s: %v", path, err)
		}
	}
	for current+1 < partitions {
		current++
		if err = flushOffset(current); err != nil {
			return err
		}
	}
	if err = bw.Flush(); err != nil {
		return fmtWrap(ErrTaskAttempt, "flush spill %s: %v", path, err)
	}
	indexPos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "seek spill %s: %v", path, err)
	}
	for _, off := range index {
		binary.BigEndian.PutUint64(indexBytes[:], off)
		if _, err = f.Write(indexBytes[:]); err != nil {
			return fmtWrap(ErrTaskAttempt, "write spill index %s: %v", path, err)
		}
	}
	binary.BigEndian.PutUint64(indexBytes[:], uint64(indexPos))
	if _, err = f.Write(indexBytes[:]); err != nil {
		return fmtWrap(ErrTaskAttempt, "write spill index %s: %v", path, err)
	}
	if err = f.Sync(); err != nil {
		return fmtWrap(ErrTaskAttempt, "sync spill %s: %v", path, err)
	}
	if err = f.Close(); err != nil {
		return fmtWrap(ErrTaskAttempt, "close spill %s: %v", path, err)
	}
	// 原子发布：spill 文件一旦出现就必须是完整的。
	if err = os.Rename(tmp, path); err != nil {
		return fmtWrap(ErrTaskAttempt, "publish spill %s: %v", path, err)
	}
	return nil
}

type recordReader struct {
	f       io.ReadCloser
	br      *bufio.Reader
	hdr     [12]byte
	seqB    [seqWidth]byte
	section *io.SectionReader
}

// openSpillPartition 打开 spill 文件并定位到指定分区的记录段。
func openSpillPartition(path string, partition, partitions int) (*recordReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmtWrap(ErrTaskAttempt, "open spill %s: %v", path, err)
	}
	r := &recordReader{f: f}
	finfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmtWrap(ErrTaskAttempt, "stat spill %s: %v", path, err)
	}
	size := finfo.Size()
	if size < 8 {
		_ = f.Close()
		return nil, fmtWrap(ErrCorruptData, "spill %s too small", path)
	}
	if _, err = f.Seek(size-8, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmtWrap(ErrTaskAttempt, "seek spill index %s: %v", path, err)
	}
	var idxBytes [8]byte
	if _, err = io.ReadFull(f, idxBytes[:]); err != nil {
		_ = f.Close()
		return nil, fmtWrap(ErrCorruptData, "read spill index %s: %v", path, err)
	}
	indexPos := int64(binary.BigEndian.Uint64(idxBytes[:]))
	indexSize := int64(partitions) * 8
	if indexPos < 0 || indexPos+indexSize+8 > size {
		_ = f.Close()
		return nil, fmtWrap(ErrCorruptData, "bad spill index in %s", path)
	}
	if _, err = f.Seek(indexPos+int64(partition)*8, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmtWrap(ErrTaskAttempt, "seek spill %s: %v", path, err)
	}
	var startB, endB [8]byte
	if _, err = io.ReadFull(f, startB[:]); err != nil {
		_ = f.Close()
		return nil, fmtWrap(ErrCorruptData, "read segment offset %s: %v", path, err)
	}
	start := int64(binary.BigEndian.Uint64(startB[:]))
	if partition+1 < partitions {
		if _, err = f.Seek(indexPos+int64(partition+1)*8, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, fmtWrap(ErrTaskAttempt, "seek spill %s: %v", path, err)
		}
		if _, err = io.ReadFull(f, endB[:]); err != nil {
			_ = f.Close()
			return nil, fmtWrap(ErrCorruptData, "read segment offset %s: %v", path, err)
		}
	} else {
		binary.BigEndian.PutUint64(endB[:], uint64(indexPos))
	}
	end := int64(binary.BigEndian.Uint64(endB[:]))
	if start < 0 || end < start || end > indexPos {
		_ = f.Close()
		return nil, fmtWrap(ErrCorruptData, "bad segment range in %s", path)
	}
	r.section = io.NewSectionReader(f, start, end-start)
	r.br = bufio.NewReaderSize(r.section, 64*1024)
	return r, nil
}

func (r *recordReader) Next() (codedRecord, error) {
	var rec codedRecord
	if _, err := io.ReadFull(r.br, r.hdr[:]); err != nil {
		if err == io.EOF {
			return rec, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return rec, fmtWrap(ErrCorruptData, "truncated record header: %v", err)
		}
		return rec, fmtWrap(ErrTaskAttempt, "read spill: %v", err)
	}
	keyLen := binary.BigEndian.Uint32(r.hdr[0:4])
	valLen := binary.BigEndian.Uint32(r.hdr[4:8])
	seqLen := binary.BigEndian.Uint32(r.hdr[8:12])
	if seqLen != seqWidth || keyLen > maxRecordLen || valLen > maxRecordLen {
		return rec, fmtWrap(ErrCorruptData, "bad record lengths key=%d val=%d seq=%d", keyLen, valLen, seqLen)
	}
	kb := make([]byte, keyLen)
	vb := make([]byte, valLen)
	if _, err := io.ReadFull(r.br, kb); err != nil {
		return rec, fmtWrap(ErrCorruptData, "truncated key: %v", err)
	}
	if _, err := io.ReadFull(r.br, vb); err != nil {
		return rec, fmtWrap(ErrCorruptData, "truncated value: %v", err)
	}
	if _, err := io.ReadFull(r.br, r.seqB[:]); err != nil {
		return rec, fmtWrap(ErrCorruptData, "truncated seq: %v", err)
	}
	var seq uint64
	for _, c := range r.seqB[:] {
		if c < '0' || c > '9' {
			return rec, fmtWrap(ErrCorruptData, "bad sequence bytes")
		}
		seq = seq*10 + uint64(c-'0')
	}
	rec.key = string(kb)
	rec.value = string(vb)
	rec.seq = seq
	return rec, nil
}

func (r *recordReader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

const maxRecordLen = 1 << 30 // 单条记录 1GiB 上限，用于识别损坏文件。

// sortRecords 按 (key, seq) 排序；seq 全局唯一，因此排序是全序、稳定确定。
func sortRecords(recs []codedRecord) {
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].key != recs[j].key {
			return recs[i].key < recs[j].key
		}
		return recs[i].seq < recs[j].seq
	})
}

// publishTempFile 在同目录内把临时文件原子改名为目标文件。
func publishTempFile(tmp, dst string) error {
	if err := os.Rename(tmp, dst); err != nil {
		return fmtWrap(ErrTaskAttempt, "publish %s: %v", filepath.Base(dst), err)
	}
	return nil
}
