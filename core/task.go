package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"sync"

	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/johanan/mvr/utils"
	"github.com/rs/zerolog/log"
)

type Task struct {
	ExecConfig *ExecutionConfig
}

func NewTask(exec ExecutionConfig) *Task {
	return &Task{
		ExecConfig: &exec,
	}
}

func SetupMv(sConfig *data.StreamConfig) (*Task, error) {
	connData := os.Getenv("MVR_SOURCE")
	if connData == "" {
		return nil, errors.New("source connection string is required")
	}
	destData := os.Getenv("MVR_DEST")
	if destData == "" {
		return nil, errors.New("destination connection string is required")
	}

	srcTemplate, err := utils.ParseTemplate("TMPL_SOURCE", connData)
	if err != nil {
		return nil, err
	}
	connStr, err := utils.ExecuteTemplate(srcTemplate, nil)
	if err != nil {
		return nil, err
	}

	destTemplate, err := utils.ParseTemplate("TMPL_DEST", destData)
	if err != nil {
		return nil, err
	}
	destStr, err := utils.ExecuteTemplate(destTemplate, nil)
	if err != nil {
		return nil, err
	}

	config := NewConfig(string(connStr), string(destStr), sConfig)
	numCores := runtime.NumCPU()
	runtime.GOMAXPROCS(numCores)
	execConfig := NewExecutionConfig(config, numCores)
	return NewTask(*execConfig), nil
}

func BuildDBReader(connURL *url.URL) (data.DBReaderConn, error) {
	source := connURL.Scheme
	var reader data.DBReaderConn
	var err error

	switch source {
	case "postgres":
		reader, err = database.NewPGDataReader(connURL)
	case "sqlserver":
		reader, err = database.NewMSDataReader(connURL)
	case "snowflake":
		reader, err = database.NewSnowflakeDataReader(connURL)
	default:
		log.Fatal().Msgf("Unsupported source: %s", source)
	}
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func Execute(ctx context.Context, concurrency int, config *data.StreamConfig, datastream *data.DataStream, reader data.DBReaderConn, writer data.DataWriter) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer writer.Close()
	errCh := make(chan error, concurrency+1)

	defer func() {
		if err := writer.Flush(); err != nil {
			log.Fatal().Msgf("Failed to flush writer: %v", err)
		}
		log.Trace().Msg("Flushed writer")
	}()

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workedId int) {
			defer wg.Done()
			if err := datastream.BatchesToWriter(ctx, writer); err != nil {
				errCh <- fmt.Errorf("worker %d: %w", i, err)
				cancel()
			}
			datastream.BatchesToWriter(ctx, writer)
			log.Trace().Int("worker", i).Msg("All batches written")
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := reader.ExecuteDataStream(ctx, datastream, config); err != nil {
			errCh <- fmt.Errorf("reader: %w", err)
			cancel()
		}
	}()

	wg.Wait()
	close(errCh)
	log.Trace().Msg("All workers have finished")

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}
