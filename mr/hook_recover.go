package mr

// recoverHookErr 执行一个钩子，把其中的 panic 归一成 ErrOperatorPanic，
// 与用户算子 panic 的处理方式保持一致。
func recoverHookErr(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &operatorError{phase: "hook", panicValue: r}
		}
	}()
	return fn()
}
