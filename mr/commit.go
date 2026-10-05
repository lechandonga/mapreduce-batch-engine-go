package mr

import (
	"os"
	"path/filepath"
)

// commitOutput 把 staging 中的 part 文件原子发布到输出目录，
// 然后写指纹文件与 _SUCCESS 标记。
//
// 提交顺序保证“只有 _SUCCESS 存在时结果才完整”：
// part 文件逐个 rename 进入输出目录（同文件系统 rename 原子）；
// 全部 part 与指纹落盘并 fsync 后，最后才创建 _SUCCESS。
// 若中途崩溃，下次运行只会看到没有 _SUCCESS 的可识别残留，
// 会被整体清理后重算，绝不会与旧半成品混合。
func commitOutput(staging, outDir, fingerprint, jobName string) error {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return fmtWrap(ErrTaskAttempt, "read staging: %v", err)
	}
	var parts []string
	for _, e := range entries {
		if isPartFileName(e.Name()) && !e.IsDir() {
			parts = append(parts, e.Name())
		}
	}
	if len(parts) == 0 {
		return fmtWrap(ErrTaskAttempt, "no reduce outputs staged")
	}
	// part 文件名含分区编号，排序提交使提交过程本身也是确定的。
	sortStrings(parts)
	for _, name := range parts {
		from := filepath.Join(staging, name)
		to := filepath.Join(outDir, name)
		if err = publishTempFile(from, to); err != nil {
			return err
		}
	}
	fpPath := filepath.Join(outDir, fingerprintFile)
	if err = writeAtomic(fpPath, []byte(serializeFingerprint(fingerprint, jobName))); err != nil {
		return err
	}
	if err = fsyncDir(outDir); err != nil {
		return err
	}
	// 最后一步：_SUCCESS 是结果完整的唯一权威标记。
	if err = writeAtomic(filepath.Join(outDir, successMarker), []byte("ok\n")); err != nil {
		return err
	}
	return fsyncDir(outDir)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
