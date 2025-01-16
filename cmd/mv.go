package cmd

import (
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

		// start timing
		start := time.Now()

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

		writer, err := file.GetPathAndIO(ctx, path, bar, sConfig.Compression, sConfig.Format)
		if err != nil {
			return fmt.Errorf("error running task: %v", err)
		}
		defer writer.Close()
		log.Info().Msgf("Writing to %s", path)

		// create datastream
		datastream, err := reader.CreateDataStream(ctx, config.SourceConn.ParsedUrl, config.StreamConfig)
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
