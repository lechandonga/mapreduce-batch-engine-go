// Package operators 提供内置 Mapper/Reducer，以及在默认注册中心登记的入口。
package operators

import (
	"strconv"
	"strings"

	"github.com/lechandonga/mapreduce-batch-engine-go/mr"
)

func init() {
	mr.MustRegisterMapper("wordcount", mr.MapperFunc(WordCountMap))
	mr.MustRegisterReducer("sum", mr.ReducerFunc(SumReduce))
}

// WordCountMap 把每行按空白切成词，输出 (word, "1")。
func WordCountMap(_, line string, out mr.MapContext) {
	for _, w := range strings.Fields(line) {
		out.Emit(w, "1")
	}
}

// SumReduce 统计同键 value 中可解析整数的个数（词频统计的求和端）。
func SumReduce(key string, ctx mr.ReduceContext) {
	var n int64
	values := ctx.Values()
	for {
		v, ok := values.Next()
		if !ok {
			break
		}
		x, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			n += x
			continue
		}
		n++ // 非数字 value 按一条记录计
	}
	ctx.Emit(key, strconv.FormatInt(n, 10))
}
