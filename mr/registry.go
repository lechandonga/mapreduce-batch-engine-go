package mr

import (
	"fmt"
	"sync"
)

// Registry 让使用方以统一方式注册 Mapper / Reducer 实现，
// 作业定义里只需给出算子名，便于同进程多作业提交。
type Registry struct {
	mu       sync.RWMutex
	mappers  map[string]Mapper
	reducers map[string]Reducer
}

// NewRegistry 创建空注册中心。
func NewRegistry() *Registry {
	return &Registry{mappers: map[string]Mapper{}, reducers: map[string]Reducer{}}
}

// RegisterMapper 注册 Mapper，重复注册同名算子直接报错，避免静默覆盖。
func (r *Registry) RegisterMapper(name string, m Mapper) error {
	if name == "" {
		return validationError(ErrMissingOperator, "mapper name is empty")
	}
	if m == nil {
		return validationError(ErrMissingOperator, "mapper %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mappers[name]; ok {
		return validationError(ErrMissingOperator, "mapper %q already registered", name)
	}
	r.mappers[name] = m
	return nil
}

// RegisterReducer 注册 Reducer。
func (r *Registry) RegisterReducer(name string, rd Reducer) error {
	if name == "" {
		return validationError(ErrMissingOperator, "reducer name is empty")
	}
	if rd == nil {
		return validationError(ErrMissingOperator, "reducer %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.reducers[name]; ok {
		return validationError(ErrMissingOperator, "reducer %q already registered", name)
	}
	r.reducers[name] = rd
	return nil
}

// Mapper 按名取出 Mapper。
func (r *Registry) Mapper(name string) (Mapper, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.mappers[name]
	if !ok {
		return nil, validationError(ErrMissingOperator, "mapper %q not registered", name)
	}
	return m, nil
}

// Reducer 按名取出 Reducer。
func (r *Registry) Reducer(name string) (Reducer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rd, ok := r.reducers[name]
	if !ok {
		return nil, validationError(ErrMissingOperator, "reducer %q not registered", name)
	}
	return rd, nil
}

// WithOperators 用注册中心的算子名填充 JobConfig，返回新的 JobConfig。
func (r *Registry) WithOperators(cfg JobConfig, mapperName, reducerName string) (JobConfig, error) {
	m, err := r.Mapper(mapperName)
	if err != nil {
		return cfg, err
	}
	rd, err := r.Reducer(reducerName)
	if err != nil {
		return cfg, err
	}
	cfg.Mapper, cfg.Reducer = m, rd
	return cfg, nil
}

var defaultRegistry = NewRegistry()

// RegisterMapper 向进程级默认注册中心注册。
func RegisterMapper(name string, m Mapper) error { return defaultRegistry.RegisterMapper(name, m) }

// RegisterReducer 向进程级默认注册中心注册。
func RegisterReducer(name string, rd Reducer) error {
	return defaultRegistry.RegisterReducer(name, rd)
}

// MustRegisterMapper 失败即 panic，仅供包初始化期使用。
func MustRegisterMapper(name string, m Mapper) {
	if err := RegisterMapper(name, m); err != nil {
		panic(fmt.Sprintf("mr: register mapper %s: %v", name, err))
	}
}

// MustRegisterReducer 失败即 panic，仅供包初始化期使用。
func MustRegisterReducer(name string, rd Reducer) {
	if err := RegisterReducer(name, rd); err != nil {
		panic(fmt.Sprintf("mr: register reducer %s: %v", name, err))
	}
}
