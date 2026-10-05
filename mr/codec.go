package mr

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// shuffle 落盘 / reduce run 文件的帧格式（小端）：
//
//	帧体长度  uint32
//	key 长度  uint32
//	key 字节  []byte
//	value 长度 uint32
//	value 字节 []byte
//	sourceMap int32   仅 shuffle 文件使用：来源 map 任务序号
//	seq       int64   仅 shuffle 文件使用：map 内记录序号
//	CRC32(IEEE) uint32，覆盖帧体其余全部字节
//
// 带 CRC 与长度定界，保证归并读到坏帧/截断帧时报 IO 错误（可重试/失败归一），
// 不会静默丢记录或重复记录。

const maxRecordBytes = 64 * 1024 * 1024 // 单条记录 64MiB 硬上限，防止坏文件撑爆内存

var errCorruptFrame = errors.New("mr: corrupt shuffle frame")

type frameHeader struct {
	key, value string
	sourceMap  int32
	seq        int64
}

func writeFrame(w io.Writer, key, value string, sourceMap int32, seq int64) error {
	kb, vb := []byte(key), []byte(value)
	bodyLen := uint32(4 + len(kb) + 4 + len(vb) + 4 + 8)
	buf := make([]byte, 0, 4+int(bodyLen)+4)
	tmp := make([]byte, 8)

	putU32 := func(v uint32) {
		binary.LittleEndian.PutUint32(tmp, v)
		buf = append(buf, tmp[:4]...)
	}

	putU32(bodyLen)
	bodyStart := len(buf)
	putU32(uint32(len(kb)))
	buf = append(buf, kb...)
	putU32(uint32(len(vb)))
	buf = append(buf, vb...)
	binary.LittleEndian.PutUint32(tmp, uint32(sourceMap))
	buf = append(buf, tmp[:4]...)
	binary.LittleEndian.PutUint64(tmp, uint64(seq))
	buf = append(buf, tmp[:8]...)
	crc := crcIEEE(buf[bodyStart:])
	putU32(crc)

	_, err := w.Write(buf)
	return err
}

func readFrame(r io.Reader) (frameHeader, error) {
	var h frameHeader
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 64*1024)
	}
	readFull := func(b []byte) error {
		_, err := io.ReadFull(br, b)
		return err
	}
	tmp := make([]byte, 8)
	if err := readFull(tmp[:4]); err != nil {
		return h, err
	}
	bodyLen := binary.LittleEndian.Uint32(tmp[:4])
	if bodyLen < 20 || bodyLen > maxRecordBytes {
		return h, errCorruptFrame
	}
	body := make([]byte, bodyLen)
	if err := readFull(body); err != nil {
		return h, err
	}
	if err := readFull(tmp[:4]); err != nil {
		return h, err
	}
	gotCRC := binary.LittleEndian.Uint32(tmp[:4])
	if crcIEEE(body) != gotCRC {
		return h, errCorruptFrame
	}

	off := 0
	take := func(n int) ([]byte, error) {
		if off+n > len(body) {
			return nil, errCorruptFrame
		}
		b := body[off : off+n]
		off += n
		return b, nil
	}
	b, err := take(4)
	if err != nil {
		return h, err
	}
	kl := int(binary.LittleEndian.Uint32(b))
	b, err = take(kl)
	if err != nil {
		return h, err
	}
	h.key = string(b)
	b, err = take(4)
	if err != nil {
		return h, err
	}
	vl := int(binary.LittleEndian.Uint32(b))
	b, err = take(vl)
	if err != nil {
		return h, err
	}
	h.value = string(b)
	b, err = take(4)
	if err != nil {
		return h, err
	}
	h.sourceMap = int32(binary.LittleEndian.Uint32(b))
	b, err = take(8)
	if err != nil {
		return h, err
	}
	h.seq = int64(binary.LittleEndian.Uint64(b))
	if off != len(body) {
		return h, errCorruptFrame
	}
	return h, nil
}
