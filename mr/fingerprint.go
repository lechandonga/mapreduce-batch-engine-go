package mr

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const successMarker = "_SUCCESS"
const fingerprintFile = "_FINGERPRINT"

// fingerprint 唯一标识一次计算的输入。它包含：
//   - 每个输入文件的（绝对路径、大小、modtime、内容 sha256）
//   - 分区数（分区数变了输出布局也变）
//
// 文件按路径排序后摘要，因此同一组输入在不同并发度、不同重试下指纹一致；
// 任意输入内容变化都会导致指纹变化，从而拒绝复用旧结果。
func computeFingerprint(spec *JobSpec) (string, error) {
	paths := append([]string(nil), spec.InputFiles...)
	sort.Strings(paths)
	h := sha256.New()
	fmt.Fprintf(h, "partitions=%d\n", spec.Partitions)
	fmt.Fprintf(h, "format=v1\n")
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return "", err
		}
		st, err := os.Stat(abs)
		if err != nil {
			return "", fmtWrap(ErrInputNotFound, "stat %s: %v", abs, err)
		}
		fh := sha256.New()
		f, err := os.Open(abs)
		if err != nil {
			return "", fmtWrap(ErrInputNotFound, "open %s: %v", abs, err)
		}
		if _, err = io.Copy(fh, f); err != nil {
			_ = f.Close()
			return "", fmtWrap(ErrTaskAttempt, "hash %s: %v", abs, err)
		}
		_ = f.Close()
		fmt.Fprintf(h, "file=%s size=%d mtime=%d sha=%s\n",
			abs, st.Size(), st.ModTime().UnixNano(), hex.EncodeToString(fh.Sum(nil)))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// parseFingerprintFile 读取已提交输出中的指纹文件。
func parseFingerprintFile(outputDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(outputDir, fingerprintFile))
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for _, ln := range lines {
		if strings.HasPrefix(ln, "fingerprint=") {
			return strings.TrimPrefix(ln, "fingerprint="), nil
		}
	}
	return "", fmt.Errorf("fingerprint file missing value")
}

func serializeFingerprint(fp, jobName string) string {
	return "version=v1\njob=" + jobName + "\nfingerprint=" + fp + "\n"
}
