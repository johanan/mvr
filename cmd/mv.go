package cmd

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/johanan/mvr/core"
	d "github.com/johanan/mvr/data"
	"github.com/johanan/mvr/file"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
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

		err = sConfig.Validate()
		if err != nil {
			return fmt.Errorf("error validating config: %v", err)
		}
		log.Debug().Interface("config", sConfig).Msg("Config")

		config, err := core.SetupConfig(sConfig)
		if err != nil {
			return fmt.Errorf("error setting up task: %v", err)
		}

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

		reader, err := core.BuildDBReader(config.SourceConn.ParsedUrl)
		if err != nil {
			return err
		}
		defer reader.Close()

		// add progress bar
		var bar *progressbar.ProgressBar
		if quiet || silent {
			bar = progressbar.DefaultBytesSilent(-1)
		} else {
			bar = file.NewProgressBar()
		}

		path, err := file.BuildFullPath(config.DestConn.ParsedUrl, sConfig.Filename)
		if err != nil {
			return fmt.Errorf("error building path: %v", err)
		}

		result := core.NewFlowResult(config.SourceConn.ParsedUrl, sConfig, time.Now()).SetPath(path)

		writer, err := file.GetPathAndIO(ctx, path, bar, sConfig.Compression, sConfig.Format)
		if err != nil {
			errFmt := fmt.Errorf("error getting path and io: %v", err)
			result.Error(errFmt.Error()).LogContext(log.Error()).Send()
			return errFmt
		}
		log.Info().Msgf("Writing to %s", path)

		// create datastream
		datastream, err := reader.CreateDataStream(ctx, config.SourceConn.ParsedUrl, config.StreamConfig)
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

		var acc error
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
