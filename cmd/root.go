package cmd

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "mvr",
	Short: "mvr is a tool for moving data",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(mvCmd)
	rootCmd.AddCommand(mvsCmd)
	rootCmd.AddCommand(execCmd)
	rootCmd.PersistentFlags().BoolP("debug", "d", false, "enable debug logging")
	rootCmd.PersistentFlags().BoolP("quiet", "q", false, "disable progress bar but keep info logging")
	rootCmd.PersistentFlags().BoolP("silent", "s", false, "disable all logging and progress bar")
	rootCmd.PersistentFlags().Int("concurrency", 0, "number of concurrent workers")
	rootCmd.PersistentFlags().Bool("console", false, "output as human readable text instead of json")
	rootCmd.PersistentFlags().String("log-level", "", "set the log level, any value that zerolog accepts. This overrides the --debug flag")
}

func Execute(ctx context.Context) error {
	rootCmd.SetContext(ctx)
	// set default log level to info
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	return rootCmd.Execute()
}
