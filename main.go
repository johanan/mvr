package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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

	if err := cmd.Execute(ctx); err != nil {
		cancel()
		log.Fatal().Msgf("Failed to execute command: %v", err)
		os.Exit(1)
	}
}
