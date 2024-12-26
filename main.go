package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/johanan/mvr/cmd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-sigChan:
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	if err := cmd.Execute(ctx); err != nil {
		os.Exit(1)
	}
}
