package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johanan/mvr/cmd"
	"github.com/rs/zerolog/log"
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

	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info().Msgf("Execution took %s", elapsed)
	}()

	if err := cmd.Execute(ctx); err != nil {
		os.Exit(1)
	}
}
