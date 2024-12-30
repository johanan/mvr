package core

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"runtime"
	"sync"

	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/johanan/mvr/file"
	"github.com/johanan/mvr/utils"
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
		log.Fatalf("Unsupported source: %s", source)
	}
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func RunMv(ctx context.Context, task *Task) error {
	source := task.ExecConfig.Config.SourceConn.ParsedUrl
	reader, err := BuildDBReader(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	datastream, err := reader.CreateDataStream(ctx, task.ExecConfig.Config.SourceConn.ParsedUrl, task.ExecConfig.Config.StreamConfig)
	if err != nil {
		return err
	}

	// get the writer
	bufWriter, err := file.GetIo(task.ExecConfig.Config.DestConn.ParsedUrl, task.ExecConfig.Config.StreamConfig)
	if err != nil {
		return err
	}

	// just a string for now
	compressedWriter, err := CreateCompressedWriter(bufWriter, task.ExecConfig.Config.StreamConfig.Compression, task.ExecConfig.Config.StreamConfig.Format)
	if err != nil {
		return err
	}
	defer func() {
		if err := compressedWriter.Close(); err != nil {
			log.Printf("Error closing compressed writer: %v\n", err)
		}
	}()

	var writer data.DataWriter
	switch task.ExecConfig.Config.StreamConfig.Format {
	case "jsonl":
		writer = file.AddJSONL(datastream, compressedWriter)
	case "csv":
		writer = file.AddCSV(datastream, compressedWriter)
	case "parquet":
		writer = file.AddParquet(datastream, compressedWriter)
	default:
		log.Fatalf("Unsupported format: %s", task.ExecConfig.Config.StreamConfig.Format)
	}

	execute(ctx, task, datastream, reader, writer)
	defer writer.Close()
	return nil
}

func CreateCompressedWriter(bufWriter *bufio.Writer, compressionType string, format string) (io.WriteCloser, error) {
	if format == "parquet" {
		return nopWriteCloser{bufWriter}, nil
	}

	if compressionType == "gzip" {
		gzipWriter := gzip.NewWriter(bufWriter)
		return &gzipWriteCloser{gzipWriter: gzipWriter, bufferedWriter: bufWriter}, nil
	}

	return nopWriteCloser{bufWriter}, nil
}

type gzipWriteCloser struct {
	gzipWriter     *gzip.Writer
	bufferedWriter *bufio.Writer
}

func (w *gzipWriteCloser) Write(p []byte) (n int, err error) {
	return w.gzipWriter.Write(p)
}

func (w *gzipWriteCloser) Close() error {
	// Flush the gzip writer to ensure all data is compressed and sent to the underlying writer
	if err := w.gzipWriter.Flush(); err != nil {
		return err
	}

	if err := w.gzipWriter.Close(); err != nil {
		return err
	}

	if err := w.bufferedWriter.Flush(); err != nil {
		return err
	}

	return nil
}

type nopWriteCloser struct {
	writer io.Writer
}

func (w nopWriteCloser) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w nopWriteCloser) Close() error {
	// If the underlying writer is a buffered writer, flush it
	if bufWriter, ok := w.writer.(*bufio.Writer); ok {
		if err := bufWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func execute(ctx context.Context, task *Task, datastream *data.DataStream, reader data.DBReaderConn, writer data.DataWriter) error {
	defer reader.Close()

	var wg sync.WaitGroup

	for i := 0; i < task.ExecConfig.Concurrency; i++ {
		wg.Add(1)
		go func(workedId int) {
			defer wg.Done()
			datastream.BatchesToWriter(writer)
		}(i)
	}

	config := task.ExecConfig.Config.StreamConfig

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := reader.ExecuteDataStream(ctx, datastream, config); err != nil {
			log.Printf("ExecuteDataStream error: %v", err)
		}
	}()

	wg.Wait()

	if err := writer.Flush(); err != nil {
		log.Fatalf("Failed to flush writer: %v", err)
	}

	return nil
}
