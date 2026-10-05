package mr_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

func TestSmokeWordCount(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	writeInputs(t, in, map[string]string{
		"a.txt":     "the quick brown fox\nthe fox\n",
		"sub/b.txt": "quick rabbit the fox",
	})

	cfg := baseConfig("smoke", in, out)
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce

	runner, err := mr.NewEngine().Submit(cfg)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	t.Logf("场景=词频统计 输入=3行2文件 判定=%+v", res.Stats)

	got := readOutput(t, out)
	want := []string{"brown\t1", "fox\t3", "quick\t2", "rabbit\t1", "the\t3"}
	if len(got) != len(want) {
		t.Fatalf("输出行数不一致 got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第%d行 got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestValidationErrors(t *testing.T) {
	root := t.TempDir()
	eng := mr.NewEngine()

	// 输入不存在
	cfg := baseConfig("bad-input", filepath.Join(root, "noin"), filepath.Join(root, "o1"))
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	_, err := eng.Submit(cfg)
	if !errors.Is(err, mr.ErrInputNotFound) {
		t.Fatalf("期望 ErrInputNotFound, got %v", err)
	}
	t.Logf("场景=输入不存在 判定=%v", err)

	// 空输入
	emptyIn := filepath.Join(root, "empty")
	os.MkdirAll(emptyIn, 0o755)
	cfg = baseConfig("empty", emptyIn, filepath.Join(root, "o2"))
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	_, err = eng.Submit(cfg)
	if !errors.Is(err, mr.ErrEmptyInput) {
		t.Fatalf("期望 ErrEmptyInput, got %v", err)
	}
	t.Logf("场景=空输入 判定=%v", err)

	// 非法分区数 / 内存预算 / 缺算子
	cfg = baseConfig("bad-parts", filepath.Join(root, "empty"), filepath.Join(root, "o3"))
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	cfg.ReducePartitions = 0
	_, err = eng.Submit(cfg)
	if !errors.Is(err, mr.ErrInvalidPartitions) {
		t.Fatalf("期望 ErrInvalidPartitions, got %v", err)
	}
	t.Logf("场景=分区数非法 判定=%v", err)

	cfg = baseConfig("bad-budget", filepath.Join(root, "empty"), filepath.Join(root, "o4"))
	cfg.Mapper, cfg.Reducer = wcMap, wcReduce
	cfg.MapMemoryBytes = 1024
	_, err = eng.Submit(cfg)
	if !errors.Is(err, mr.ErrInvalidBudget) {
		t.Fatalf("期望 ErrInvalidBudget, got %v", err)
	}
	t.Logf("场景=内存预算非法 判定=%v", err)

	cfg = baseConfig("no-op", filepath.Join(root, "empty"), filepath.Join(root, "o5"))
	_, err = eng.Submit(cfg)
	if !errors.Is(err, mr.ErrMissingOperator) {
		t.Fatalf("期望 ErrMissingOperator, got %v", err)
	}
	t.Logf("场景=缺少算子 判定=%v", err)
}

func TestIdempotentReuseAndConflict(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "in")
	out := filepath.Join(root, "out")
	writeInputs(t, in, map[string]string{"a.txt": "a b c a"})

	runOnce := func(id string) (*mr.Result, error) {
		cfg := baseConfig(id, in, out)
		cfg.Mapper, cfg.Reducer = wcMap, wcReduce
		r, err := mr.NewEngine().Submit(cfg)
		if err != nil {
			return nil, err
		}
		return r.Run(context.Background())
	}

	res1, err := runOnce("idem-1")
	if err != nil {
		t.Fatal(err)
	}
	res2, err := runOnce("idem-2")
	if err != nil {
		t.Fatalf("重跑应复用: %v", err)
	}
	if !res2.Stats.ReusedCommitted || res1.Stats.OutputRecords != 3 {
		t.Fatalf("复用标记异常 res1=%+v res2=%+v", res1.Stats, res2.Stats)
	}
	t.Logf("场景=同输入重跑 判定=第二次复用已提交结果 res1=%+v res2=%+v", res1.Stats, res2.Stats)

	// 输入变化 => 指纹冲突，必须拒绝
	writeInputs(t, in, map[string]string{"a.txt": "a b c a a a"})
	_, err = runOnce("idem-3")
	if !errors.Is(err, mr.ErrOutputConflict) {
		t.Fatalf("期望 ErrOutputConflict, got %v", err)
	}
	t.Logf("场景=输入变化复用旧输出 判定=拒绝: %v", err)

	// 半成品输出（无 _SUCCESS）默认拒绝
	os.Remove(filepath.Join(out, "_SUCCESS"))
	_, err = runOnce("idem-4")
	if !errors.Is(err, mr.ErrUncommittedOutput) {
		t.Fatalf("期望 ErrUncommittedOutput, got %v", err)
	}
	t.Logf("场景=半成品输出 判定=拒绝: %v", err)
}
