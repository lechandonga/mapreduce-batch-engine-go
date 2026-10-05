// Command wordcount 是引擎自带的示例作业：对若干文本文件做词频统计。
//
// 用法：
//
//	go run ./cmd/wordcount -in a.txt -in b.txt -out ./wc-out \
//	    -partitions 4 -workers 2 -shuffle-bytes 65536
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

func main() {
	var inputs multiFlag
	flag.Var(&inputs, "in", "输入文本文件（可重复指定）")
	out := flag.String("out", "", "输出目录")
	partitions := flag.Int("partitions", 4, "Reduce 分区数")
	workers := flag.Int("workers", 2, "并发 worker 数")
	budget := flag.Int("shuffle-bytes", 4<<20, "每个 shuffle 收集器的内存预算（字节）")
	attempts := flag.Int("attempts", 3, "单任务最大尝试次数")
	flag.Parse()

	if len(inputs) == 0 || *out == "" {
		fmt.Fprintln(os.Stderr, "必须指定至少一个 -in 和一个 -out")
		os.Exit(2)
	}

	spec := &mr.JobSpec{
		Name:         "wordcount",
		InputFiles:   inputs,
		OutputDir:    *out,
		Partitions:   *partitions,
		Workers:      *workers,
		MaxAttempts:  *attempts,
		ShuffleBytes: *budget,
		MapFn:        mapWords,
		ReduceFn:     sumCounts,
	}
	res, err := mr.Run(context.Background(), spec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "作业失败: %v\n", err)
		switch {
		case errors.Is(err, mr.ErrInputNotFound):
			os.Exit(3)
		case errors.Is(err, mr.ErrOutputConflict), errors.Is(err, mr.ErrFingerprintMismatch):
			os.Exit(4)
		case errors.Is(err, mr.ErrAttemptBudgetExhausted):
			os.Exit(5)
		default:
			os.Exit(1)
		}
	}
	if res.Reused {
		fmt.Println("复用已提交结果，未重新计算")
	} else {
		fmt.Printf("完成：map 尝试 %d 次，reduce 尝试 %d 次\n", res.MapAttempts, res.ReduceAttempts)
	}
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// mapWords 把一行文本拆成单词，每个词发射计数 1。
func mapWords(_ context.Context, _ string, line string, emit mr.Emitter) error {
	for _, w := range strings.Fields(line) {
		w = strings.ToLower(strings.Trim(w, ".,!?;:\"'()[]{}"))
		if w == "" {
			continue
		}
		if err := emit.Emit(w, "1"); err != nil {
			return err
		}
	}
	return nil
}

// sumCounts 对一个词的所有计数求和，发射唯一的最终值。
func sumCounts(_ context.Context, key string, values mr.ValueIterator, emit mr.Emitter) error {
	total := 0
	for {
		v, ok, err := values.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("wordcount: bad count %q: %w", v, err)
		}
		total += n
	}
	return emit.Emit(key, strconv.Itoa(total))
}
