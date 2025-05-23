package cmd

import (
	"errors"
	"fmt"
	"os"
	"runtime"
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

var (
	mvsFormat      string
	mvsFilename    string
	mvsCompression string
	mvsCfgFile     string
	mvsSelect      string
)

var mvsCmd = &cobra.Command{
	Use:   "mvs",
	Short: "mvs runs multiple mv commands",
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
				log.Warn().Msgf("not a valid log level: %s", logLevel)
			}
			zerolog.SetGlobalLevel(lvl)
		}

		console, _ := cmd.Flags().GetBool("console")
		if console {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
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

		config, err := core.SetupConfig(&multiConfig.StreamConfig)
		if err != nil {
			return fmt.Errorf("error setting up task: %v", err)
		}

		reader, err := core.BuildDBReader(config.SourceConn.ParsedUrl)
		if err != nil {
			return err
		}
		defer reader.Close()

		silent, _ := cmd.Flags().GetBool("silent")
		quiet, _ := cmd.Flags().GetBool("quiet")
		cliConcurrency, _ := cmd.Flags().GetInt("concurrency")

		var concurrency int
		if cliConcurrency > 0 {
			concurrency = cliConcurrency
		} else {
			concurrency = runtime.NumCPU()
		}

		if silent {
			zerolog.SetGlobalLevel(zerolog.Disabled)
		}

		tablesToProcess := filterTables(multiConfig.Tables, mvsSelect)

		for i, table := range tablesToProcess {
			var acc error
			log.Info().Msgf("Starting %d/%d", i+1, len(tablesToProcess))
			// invert so that each table can override the root
			sConfig, err := data.BuildConfig(multiBytes, &table)
			if err != nil {
				return fmt.Errorf("error building config: %v", err)
			}

			err = sConfig.Validate()
			if err != nil {
				return fmt.Errorf("error validating config: %v", err)
			}
			log.Debug().Interface("config", sConfig).Msg("Config")

			path, err := file.BuildFullPath(config.DestConn.ParsedUrl, sConfig.Filename)
			if err != nil {
				return fmt.Errorf("error building path: %v", err)
			}

			result := core.NewFlowResult(config.SourceConn.ParsedUrl, sConfig, time.Now()).SetPath(path)

			var bar *progressbar.ProgressBar
			if quiet || silent {
				bar = progressbar.DefaultBytesSilent(-1)
			} else {
				bar = file.NewProgressBar()
			}

			writer, err := file.GetPathAndIO(ctx, path, bar, sConfig.Compression, sConfig.Format)
			if err != nil {
				errFmt := fmt.Errorf("error getting path and IO: %v", err)
				result.Error(errFmt.Error()).LogContext(log.Error()).Send()
				return errFmt
			}

			log.Info().Msgf("Writing to %s", path)

			datastream, err := reader.CreateDataStream(ctx, config.SourceConn.ParsedUrl, sConfig)
			if err != nil {
				result.Error(err.Error()).LogContext(log.Error()).Send()
				return err
			}

			fileWriter, err := file.AddFileWriter(sConfig.Format, datastream, writer)
			if err != nil {
				result.Error(err.Error()).LogContext(log.Error()).Send()
				return err
			}

			err = core.Execute(ctx, concurrency, sConfig, datastream, reader, fileWriter)
			if err != nil {
				result.Error(err.Error()).LogContext(log.Error()).Send()
				return err
			}

			if err := fileWriter.Flush(); err != nil {
				acc = errors.Join(acc, fmt.Errorf("flush writer: %w", err))

			}
			if err := fileWriter.Close(); err != nil {
				acc = errors.Join(acc, fmt.Errorf("close writer: %w", err))

			}
			if err := writer.Close(); err != nil {
				acc = errors.Join(acc, fmt.Errorf("close writer: %w", err))
			}

			if acc != nil {
				result.Error(acc.Error()).LogContext(log.Error()).Send()
				return acc
			}
			log.Trace().Msg("Flushed writer")

			result.SetRows(datastream.TotalRows).SetBytes(bar.State().CurrentBytes).Success()
			result.LogContext(log.Info()).Msg("Finished writing data")
		}

		return nil
	},
}

func filterTables(tables []data.StreamConfig, selectList string) []data.StreamConfig {
	if selectList == "" {
		return tables
	}

	splitList := strings.Split(strings.ToLower(selectList), ",")
	splitSet := make(map[string]struct{})
	for _, s := range splitList {
		splitSet[s] = struct{}{}
	}

	var selected []data.StreamConfig
	for _, table := range tables {
		if _, ok := splitSet[strings.ToLower(table.StreamName)]; ok {
			selected = append(selected, table)
		}
	}

	return selected
}

func init() {
	mvsCmd.Flags().StringVar(&mvsCfgFile, "config", "", "Configuration file")
	mvsCmd.Flags().StringVar(&mvsFormat, "format", "", "Format of the data")
	mvsCmd.Flags().StringVar(&mvsFilename, "filename", "", "Name of the file")
	mvsCmd.Flags().StringVar(&mvsCompression, "compression", "", "Compression of the file")
	mvsCmd.Flags().StringVar(&mvsSelect, "select", "", "Comma separated list of which tables to process")
	mvsCmd.MarkFlagRequired("config")
}
