package mr

import (
	"os"
	"path/filepath"
)

// writeAtomic 以 “临时文件 + fsync + 同目录 rename” 的方式发布文件，
// 任何读到目标路径的一方看到的内容必然完整。
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, filepath.Base(path)+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "create %s: %v", path, err)
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmtWrap(ErrTaskAttempt, "write %s: %v", path, err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmtWrap(ErrTaskAttempt, "sync %s: %v", path, err)
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmtWrap(ErrTaskAttempt, "close %s: %v", path, err)
	}
	if err = publishTempFile(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
