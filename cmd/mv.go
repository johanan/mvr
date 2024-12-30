package cmd

import (
	"fmt"
	"os"

	"github.com/johanan/mvr/core"
	d "github.com/johanan/mvr/data"
	"github.com/spf13/cobra"
)

var mvCfgFile string
var mvFormat string
var mvFilename string
var mvSql string
var mvCompression string
var mvName string

var mvCmd = &cobra.Command{
	Use:   "mv",
	Short: "mv is what mvs the data",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

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
}
