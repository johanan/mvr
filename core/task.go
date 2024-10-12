package core

import (
	"context"
	"sync"
)

type Context struct {
	Ctx    context.Context
	Cancel context.CancelFunc
	Mux    *sync.Mutex
}

type Task struct {
	ExecConfig *ExecutionConfig
	Ctx        *Context
}

func NewContext(parent context.Context) *Context {
	ctx, cancel := context.WithCancel(parent)
	return &Context{
		Ctx:    ctx,
		Cancel: cancel,
		Mux:    &sync.Mutex{},
	}
}

func NewTask(exec ExecutionConfig) *Task {
	return &Task{
		ExecConfig: &exec,
		Ctx:        NewContext(context.Background()),
	}
}
