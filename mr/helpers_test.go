package mr_test

import (
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

func writeInputs(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// readOutput 读取 part-r-* 文件为有序的 key->value 列表。
func readOutput(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, en := range entries {
		if !strings.HasPrefix(en.Name(), "part-r-") {
			continue
		}
		f, err := os.Open(filepath.Join(dir, en.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		f.Close()
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(lines)
	return lines
}

func baseConfig(id, in, out string) mr.JobConfig {
	return mr.JobConfig{
		ID:                id,
		InputDir:          in,
		OutputDir:         out,
		ReducePartitions:  4,
		Parallelism:       2,
		MaxAttempts:       3,
		MapMemoryBytes:    256 * 1024,
		ReduceMemoryBytes: 256 * 1024,
		TempRoot:          filepath.Join(os.TempDir(), "mr-test-"+id),
	}
}

var wcMap = mr.MapperFunc(func(_, line string, out mr.MapContext) {
	for _, w := range strings.Fields(line) {
		out.Emit(w, "1")
	}
})

var wcReduce = mr.ReducerFunc(func(key string, ctx mr.ReduceContext) {
	var n int64
	values := ctx.Values()
	for {
		v, ok := values.Next()
		if !ok {
			break
		}
		if x, err := strconv.ParseInt(v, 10, 64); err == nil {
			n += x
		} else {
			n++
		}
	}
	ctx.Emit(key, strconv.FormatInt(n, 10))
})
