package cmd

import (
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

var mvsFormat string
var mvsFilename string
var mvsCompression string
var mvsCfgFile string
var mvsSelect string

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
			log.Info().Msgf("Starting %d/%d", i+1, len(tablesToProcess))
			// invert so that each table can override the root
			sConfig, err := data.BuildConfig(multiBytes, &table)
			if err != nil {
				return fmt.Errorf("error parsing template: %v", err)
			}

			err = sConfig.Validate()
			if err != nil {
				return fmt.Errorf("error validating config: %v", err)
			}
			log.Debug().Interface("config", sConfig).Msg("Config")

			start := time.Now()

			var bar *progressbar.ProgressBar
			if quiet || silent {
				bar = progressbar.DefaultBytesSilent(-1)
			} else {
				bar = file.NewProgressBar()
			}

			path, writer, err := file.GetPathAndIO(config.DestConn.ParsedUrl, bar, sConfig.Filename, sConfig.Compression, sConfig.Format)
			if err != nil {
				return fmt.Errorf("error running task: %v", err)
			}
			defer writer.Close()
			log.Info().Msgf("Writing to %s", path)

			datastream, err := reader.CreateDataStream(ctx, config.SourceConn.ParsedUrl, sConfig)
			if err != nil {
				return err
			}

			fileWriter, err := file.AddFileWriter(sConfig.Format, datastream, writer)
			if err != nil {
				return err
			}

			core.Execute(ctx, concurrency, sConfig, datastream, reader, fileWriter)
			fileWriter.Close()
			bar.Finish()
			elapsed := time.Since(start)
			log.Info().
				Str("source", config.SourceConn.ParsedUrl.Host).
				Str("sql", sConfig.SQL).
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
