package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/johanan/mvr/core"
	d "github.com/johanan/mvr/data"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var mvCfgFile string
var mvFormat string
var mvFilename string
var mvSql string
var mvCompression string
var mvName string
var mvBatchSize int

var mvCmd = &cobra.Command{
	Use:   "mv",
	Short: "mv is what mvs the data",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		debug, _ := cmd.Flags().GetBool("debug")
		if debug {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		} else {
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		}

		// check environment for trace flag
		if strings.ToLower(os.Getenv("MVR_TRACE")) == "true" {
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
		}

		var templateData []byte
		var err error

		if mvCfgFile != "" {
			templateData, err = os.ReadFile(mvCfgFile)
			if err != nil {
				return fmt.Errorf("error reading template file: %v", err)
			}
		} else {
			templateData = []byte("")
		}
		cliArgs := &d.StreamConfig{
			Format:      mvFormat,
			Filename:    mvFilename,
			SQL:         mvSql,
			Compression: mvCompression,
			StreamName:  mvName,
		}

		sConfig, err := d.BuildConfig(templateData, cliArgs)
		if err != nil {
			return fmt.Errorf("error parsing template: %v", err)
		}

		task, err := core.SetupMv(sConfig)
		if err != nil {
			return fmt.Errorf("error setting up task: %v", err)
		}
		err = core.RunMv(ctx, task)
		if err != nil {
			return fmt.Errorf("error running task: %v", err)
		}
		return nil
	},
}

func init() {
	mvCmd.Flags().StringVarP(&mvCfgFile, "config", "f", "", "config file")
	mvCmd.Flags().StringVar(&mvFormat, "format", "", "output file format")
	mvCmd.Flags().StringVar(&mvFilename, "filename", "", "output file name")
	mvCmd.Flags().StringVar(&mvSql, "sql", "", "sql query to run")
	mvCmd.Flags().StringVar(&mvCompression, "compression", "", "compression type")
	mvCmd.Flags().StringVar(&mvName, "name", "", "stream name")
	mvCmd.Flags().IntVar(&mvBatchSize, "batch-size", 0, "batch size")
}
