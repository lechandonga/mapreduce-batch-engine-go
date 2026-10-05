package mr

import (
	"os"
	"syscall"
)

// dirLock 对目录使用 flock(LOCK_EX|LOCK_NB) 做跨进程互斥。
// 第二个并发运行会立刻拿到 ErrConcurrentRun，而不是等待，
// 从而避免两个实例向同一输出位置提交造成混写。
type dirLock struct {
	f *os.File
}

func acquireDirLock(dir string) (*dirLock, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, fmtWrap(ErrTaskAttempt, "open lock dir: %v", err)
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmtWrap(ErrConcurrentRun, "lock held by another process on %s", dir)
		}
		return nil, fmtWrap(ErrTaskAttempt, "acquire lock: %v", err)
	}
	return &dirLock{f: f}, nil
}

func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	return l.f.Close()
}
