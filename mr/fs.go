package mr

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// syncDir 对目录做 fsync，保证新建/rename 的目录项落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeOutputLine 写最终输出：tab 分隔、换行结尾；\t、\n、\ 做反斜杠转义。
func writeOutputLine(w *bufio.Writer, key, value string) error {
	if _, err := w.WriteString(escapeField(key)); err != nil {
		return err
	}
	if err := w.WriteByte('\t'); err != nil {
		return err
	}
	if _, err := w.WriteString(escapeField(value)); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

func escapeField(s string) string {
	if !strings.ContainsAny(s, "\\\t\n\r") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// UnescapeOutputField 供使用方读取输出文件时还原字段。
func UnescapeOutputField(s string) (string, error) {
	if !strings.Contains(s, `\`) {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i >= len(s) {
			return "", strconv.ErrSyntax
		}
		switch s[i] {
		case '\\':
			b.WriteByte('\\')
		case 't':
			b.WriteByte('\t')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		default:
			return "", strconv.ErrSyntax
		}
	}
	return b.String(), nil
}
