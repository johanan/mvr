package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/johanan/mvr/core"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var execCmd = &cobra.Command{
	Use:   "exec",
	Short: "Executes a DB command",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		debug, _ := cmd.Flags().GetBool("debug")
		if debug {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}

		logLevel, _ := cmd.Flags().GetString("log-level")
		if logLevel != "" {
			lvl, err := zerolog.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("invalid log level: %s", logLevel)
			}
			zerolog.SetGlobalLevel(lvl)
		}

		console, _ := cmd.Flags().GetBool("console")
		if console {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		}

		sqlCommand, err := cmd.Flags().GetString("sql")
		if err != nil {
			return fmt.Errorf("failed to get sql-command flag: %w", err)
		}

		connData := os.Getenv("MVR_SOURCE")
		if connData == "" {
			return errors.New("source connection string is required")
		}

		src, err := url.Parse(connData)
		if err != nil {
			return fmt.Errorf("failed to parse source connection string: %w", err)
		}

		exec, err := core.BuildDbExec(src)
		if err != nil {
			return fmt.Errorf("failed to build DB exec: %w", err)
		}

		status, err := exec.ExecuteCommand(ctx, sqlCommand)
		if err != nil {
			return fmt.Errorf("failed to execute command: %w", err)
		}

		os.Stdout.WriteString(status)
		return nil
	},
}

func init() {
	execCmd.Flags().String("sql", "", "SQL command to execute")
}
