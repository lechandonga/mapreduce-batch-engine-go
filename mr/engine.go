package mr

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	minMemoryBytes  = 64 * 1024
	maxPartitions   = 100000
	successFileName = "_SUCCESS"
	defaultAttempts = 3
)

var jobIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// inputFileInfo 是输入分片的确定描述。
type inputFileInfo struct {
	RelPath string `json:"rel_path"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mod_time_unix_nano"`
	SHA256  string `json:"sha256"`
}

// CommitMarker 是输出目录 _SUCCESS 文件的内容。
type CommitMarker struct {
	JobID       string          `json:"job_id"`
	Fingerprint string          `json:"fingerprint"`
	Partitions  int             `json:"partitions"`
	Mapper      string          `json:"mapper"`
	Reducer     string          `json:"reducer"`
	CreatedAt   time.Time       `json:"created_at"`
	Inputs      []inputFileInfo `json:"inputs"`
	Stats       Stats           `json:"stats"`
}

// Engine 是本地 MapReduce 执行器。零值字段使用默认值。
type Engine struct {
	// Logger 用于输出场景化日志，默认写 stderr；可置 io.Discard 静音。
	Logger *log.Logger
}

// NewEngine 创建执行器。
func NewEngine() *Engine {
	return &Engine{Logger: log.New(os.Stderr, "[mr] ", log.LstdFlags|log.Lmicroseconds)}
}

// NewJob 在写出任何数据之前完成全部静态校验，返回可执行作业。
func (e *Engine) NewJob(cfg JobConfig) (*Job, error) {
	if !jobIDPattern.MatchString(cfg.ID) {
		return nil, validationError(ErrInvalidJobID, "job id %q must match %s", cfg.ID, jobIDPattern.String())
	}
	if cfg.Mapper == nil || cfg.Reducer == nil {
		return nil, validationError(ErrMissingOperator, "both Mapper and Reducer must be set")
	}
	if cfg.ReducePartitions < 1 || cfg.ReducePartitions > maxPartitions {
		return nil, validationError(ErrInvalidPartitions, "ReducePartitions=%d must be in [1,%d]", cfg.ReducePartitions, maxPartitions)
	}
	if cfg.MapMemoryBytes < minMemoryBytes {
		return nil, validationError(ErrInvalidBudget, "MapMemoryBytes=%d must be >= %d", cfg.MapMemoryBytes, minMemoryBytes)
	}
	if cfg.ReduceMemoryBytes < minMemoryBytes {
		return nil, validationError(ErrInvalidBudget, "ReduceMemoryBytes=%d must be >= %d", cfg.ReduceMemoryBytes, minMemoryBytes)
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultAttempts
	}

	info, err := os.Stat(cfg.InputDir)
	if err != nil || !info.IsDir() {
		return nil, validationError(ErrInputNotFound, "input directory %q: %v", cfg.InputDir, err)
	}
	files, err := listInputFiles(cfg.InputDir)
	if err != nil {
		return nil, validationError(ErrInputNotFound, "scan input directory %q: %v", cfg.InputDir, err)
	}
	if len(files) == 0 {
		return nil, validationError(ErrEmptyInput, "no regular files under %q", cfg.InputDir)
	}

	// 输出位置冲突在写出任何数据之前判定。
	if err := precheckOutput(cfg); err != nil {
		return nil, err
	}

	// 计算输入指纹（读取全部输入内容做摘要）。
	inputs := make([]inputFileInfo, 0, len(files))
	for _, rel := range files {
		fi, err := fingerprintFile(cfg.InputDir, rel)
		if err != nil {
			return nil, validationError(ErrInputNotFound, "fingerprint %q: %v", rel, err)
		}
		inputs = append(inputs, fi)
	}
	fp := computeFingerprint(cfg, inputs)

	return &Job{cfg: cfg, fingerprint: fp, inputFiles: files}, nil
}

// Submit 是统一提交入口：校验并返回可 Run 的作业句柄。
func (e *Engine) Submit(cfg JobConfig) (*Runner, error) {
	job, err := e.NewJob(cfg)
	if err != nil {
		return nil, err
	}
	return &Runner{engine: e, job: job}, nil
}

func listInputFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func fingerprintFile(root, rel string) (inputFileInfo, error) {
	abs := filepath.Join(root, filepath.FromSlash(rel))
	st, err := os.Stat(abs)
	if err != nil {
		return inputFileInfo{}, err
	}
	f, err := os.Open(abs)
	if err != nil {
		return inputFileInfo{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return inputFileInfo{}, err
	}
	return inputFileInfo{
		RelPath: rel,
		Size:    st.Size(),
		ModTime: st.ModTime().UnixNano(),
		SHA256:  hex.EncodeToString(h.Sum(nil)),
	}, nil
}

func computeFingerprint(cfg JobConfig, inputs []inputFileInfo) string {
	h := sha256.New()
	enc := json.NewEncoder(h)
	_ = enc.Encode(struct {
		Partitions int
		Mapper     string
		Reducer    string
		Inputs     []inputFileInfo
	}{
		Partitions: cfg.ReducePartitions,
		Mapper:     typeName(cfg.Mapper),
		Reducer:    typeName(cfg.Reducer),
		Inputs:     inputs,
	})
	return hex.EncodeToString(h.Sum(nil))
}

func typeName(v any) string {
	// 函数字面量取类型名；注册式命名算子可在 JobConfig 之外通过包装携带名称，
	// 默认指纹以 Go 类型身份为准，同一二进制内稳定。
	return fmt.Sprintf("%T", v)
}

// precheckOutput 在任何写出动作之前判定输出位置。
//
//   - 输出目录不存在：通过；
//   - 目录含 _SUCCESS：通过（指纹一致性在 run 开头比对，指纹不同即拒绝）；
//   - 目录非空但无 _SUCCESS（半成品）：默认拒绝，OverwriteUncommitted=true 时允许（运行时清空）。
func precheckOutput(cfg JobConfig) error {
	entries, err := os.ReadDir(cfg.OutputDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return validationError(ErrOutputConflict, "stat output %q: %v", cfg.OutputDir, err)
	}
	if _, err := os.Stat(filepath.Join(cfg.OutputDir, successFileName)); err == nil {
		return nil
	}
	visible := entries[:0]
	for _, en := range entries {
		// .staging-* 是（可能并发的）尝试私有暂存，不属于“半成品输出”。
		if !strings.HasPrefix(en.Name(), ".staging-") {
			visible = append(visible, en)
		}
	}
	if len(visible) > 0 && !cfg.OverwriteUncommitted {
		return &ValidationError{
			Reason: ErrUncommittedOutput,
			Detail: fmt.Sprintf("%q exists without %s; set OverwriteUncommitted=true to replace it",
				cfg.OutputDir, successFileName),
		}
	}
	return nil
}

func randomSuffix() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (e *Engine) infof(format string, args ...any) {
	if e.Logger != nil {
		e.Logger.Output(2, fmt.Sprintf(format, args...))
	}
}
