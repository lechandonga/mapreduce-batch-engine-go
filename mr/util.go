package mr

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

func fmtWrap(err error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", err, fmt.Sprintf(format, args...))
}

// partitionForKey 是全局唯一的分区函数，与并发度、文件分布无关：
// FNV-1a 32 位哈希对分区数取模，保证同一键永远进入同一分组。
func partitionForKey(key string, partitions int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(partitions))
}

// shuffle record 文件采用自描述的长度前缀格式：
//
//	uint32 keyLen
//	uint32 valLen
//	uint32 seqLen   （map 输出时携带的全局顺序号，reduce 落盘时为 0）
//	bytes  key
//	bytes  value
//	bytes  seq      （大端 uint64 的十进制 ASCII，定长 20 字节）
//
// 顺序号让 reduce 侧在同一 (mapTaskID, 序号) 维度上获得确定的归并顺序，
// 从而无论并发度如何，每个键分组内的值序列完全一致。
const seqWidth = 20

func encodeRecord(dst []byte, key, value string, seq uint64) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(key)))
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(value)))
	dst = binary.BigEndian.AppendUint32(dst, seqWidth)
	dst = append(dst, key...)
	dst = append(dst, value...)
	var seqBuf [seqWidth]byte
	s := []byte(fmt.Sprintf("%020d", seq))
	copy(seqBuf[:], s)
	dst = append(dst, seqBuf[:]...)
	return dst
}

// encodedLen 返回记录编码后的字节数。
func encodedLen(key, value string) int {
	return 12 + len(key) + len(value) + seqWidth
}

type codedRecord struct {
	key, value string
	seq        uint64
}

// recordWriter 顺序追加编码记录。
type recordWriter struct {
	buf []byte
}

func (w *recordWriter) append(key, value string, seq uint64) []byte {
	w.buf = w.buf[:0]
	w.buf = encodeRecord(w.buf, key, value, seq)
	return w.buf
}
