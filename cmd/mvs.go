package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/file"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var mvsFormat string
var mvsFilename string
var mvsCompression string
var mvsCfgFile string

var mvsCmd = &cobra.Command{
	Use:   "mvs",
	Short: "mvs runs multiple mv commands",
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

		var multiConfig data.MultiStreamConfig
		fileData, err := os.ReadFile(mvsCfgFile)
		if err != nil {
			return fmt.Errorf("error reading template file: %v", err)
		}
		if err := yaml.Unmarshal(fileData, &multiConfig); err != nil {
			return fmt.Errorf("error parsing raw yaml: %v", err)
		}

		cliArgs := data.StreamConfig{
			Format:      mvsFormat,
			Filename:    mvsFilename,
			Compression: mvsCompression,
		}

		multiConfig.OverrideValues(&cliArgs)
		multiBytes, err := yaml.Marshal(multiConfig.StreamConfig)
		if err != nil {
			return fmt.Errorf("error marshalling config: %v", err)
		}

		task, err := core.SetupMv(&multiConfig.StreamConfig)
		if err != nil {
			return fmt.Errorf("error setting up task: %v", err)
		}

		reader, err := core.BuildDBReader(task.ExecConfig.Config.SourceConn.ParsedUrl)
		if err != nil {
			return err
		}
		defer reader.Close()

		silent, _ := cmd.Flags().GetBool("silent")
		quiet, _ := cmd.Flags().GetBool("quiet")
		task.ExecConfig.Silent = silent
		task.ExecConfig.Quiet = quiet

		if silent {
			zerolog.SetGlobalLevel(zerolog.Disabled)
		}

		for i, table := range multiConfig.Tables {
			log.Info().Msgf("Starting %d/%d", i+1, len(multiConfig.Tables))
			// invert so that each table can override the root
			sConfig, err := data.BuildConfig(multiBytes, &table)
			if err != nil {
				return fmt.Errorf("error parsing template: %v", err)
			}

			err = sConfig.Validate()
			if err != nil {
				return fmt.Errorf("error validating config: %v", err)
			}

			start := time.Now()

			var bar *progressbar.ProgressBar
			if quiet || silent {
				bar = progressbar.DefaultBytesSilent(-1)
			} else {
				bar = file.NewProgressBar()
			}

			path, writer, err := file.GetPathAndIO(task.ExecConfig.Config.DestConn.ParsedUrl, bar, sConfig.Filename, sConfig.Compression, sConfig.Format)
			if err != nil {
				return fmt.Errorf("error running task: %v", err)
			}
			defer writer.Close()
			log.Info().Msgf("Writing to %s", path)

			datastream, err := reader.CreateDataStream(ctx, task.ExecConfig.Config.SourceConn.ParsedUrl, sConfig)
			if err != nil {
				return err
			}

			fileWriter, err := file.AddFileWriter(sConfig.Format, datastream, writer)
			if err != nil {
				return err
			}

			core.Execute(ctx, task.ExecConfig.Concurrency, sConfig, datastream, reader, fileWriter)
			fileWriter.Close()
			bar.Finish()
			elapsed := time.Since(start)
			log.Info().
				Str("path", path.String()).
				Int("rows", datastream.TotalRows).
				Dur("elapsed", elapsed).
				Str("duration", elapsed.String()).
				Float64("bytes", bar.State().CurrentBytes).
				Msg("Finished writing data")
		}

		return nil
	},
}

func init() {
	mvsCmd.Flags().StringVar(&mvsCfgFile, "config", "", "Configuration file")
	mvsCmd.Flags().StringVar(&mvsFormat, "format", "", "Format of the data")
	mvsCmd.Flags().StringVar(&mvsFilename, "filename", "", "Name of the file")
	mvsCmd.Flags().StringVar(&mvsCompression, "compression", "", "Compression of the file")
	mvsCmd.MarkFlagRequired("config")
}
