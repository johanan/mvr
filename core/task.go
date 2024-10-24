package core

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log"
	"net/url"
	"runtime"
	"sync"

	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/johanan/mvr/file"
	"github.com/spf13/viper"
)

type Context struct {
	Ctx    context.Context
	Cancel context.CancelFunc
	Mux    *sync.Mutex
}

type Task struct {
	ExecConfig *ExecutionConfig
	Ctx        *Context
}

func NewContext(parent context.Context) *Context {
	ctx, cancel := context.WithCancel(parent)
	return &Context{
		Ctx:    ctx,
		Cancel: cancel,
		Mux:    &sync.Mutex{},
	}
}

func NewTask(exec ExecutionConfig) *Task {
	return &Task{
		ExecConfig: &exec,
		Ctx:        NewContext(context.Background()),
	}
}

func SetupDb2File(sConfig *StreamConfig) (*Task, error) {
	connStr := viper.GetString("source")
	if connStr == "" {
		return nil, errors.New("source connection string is required")
	}
	destStr := viper.GetString("dest")
	if destStr == "" {
		return nil, errors.New("destination connection string is required")
	}

	config := NewConfig(connStr, destStr, sConfig)
	numCores := runtime.NumCPU()
	runtime.GOMAXPROCS(numCores)
	execConfig := NewExecutionConfig(config, numCores)
	return NewTask(*execConfig), nil
}

func RunDb2File(task *Task) error {
	datastream, db := database.CreateDataStream(task.ExecConfig.Config.SourceConn.ParsedUrl, task.ExecConfig.ReaderOptions.Sql)
	task.ExecConfig.ReaderConn = db
	defer db.Close()

	// just a string for now
	compressedWriter, err := CreateCompressedWriter(task.ExecConfig.Config.DestConn.ParsedUrl, task.ExecConfig.Config.StreamConfig.Compression, task.ExecConfig.Config.StreamConfig.Format)
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

	execute(task, datastream, writer)
	defer writer.Close()
	return nil
}

func CreateCompressedWriter(outputURL *url.URL, compressionType string, format string) (io.WriteCloser, error) {
	bufWriter, err := file.GetIo(outputURL)
	if err != nil {
		return nil, err
	}

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

func execute(task *Task, datastream *data.DataStream, writer data.DataWriter) {
	var readWg = task.ExecConfig.ReaderWg
	var writeWg = task.ExecConfig.WriterWg

	columns := datastream.Columns

	for i := 0; i < task.ExecConfig.Concurrency; i++ {
		writeWg.Add(1)
		go datastream.BatchesToWriter(writeWg, writer)
	}

	db := task.ExecConfig.ReaderConn
	query := task.ExecConfig.ReaderOptions.Sql

	// Query the database and produce batches
	result, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	batch := data.Batch{Rows: make([][]any, 0, datastream.BatchSize)}

	readWg.Add(1)
	go func() {
		defer readWg.Done()
		for result.Next() {
			row := make([]any, len(columns))
			rowPtrs := make([]any, len(columns))
			for i := range row {
				rowPtrs[i] = &row[i]
			}
			err = result.Scan(rowPtrs...)
			if err != nil {
				log.Fatalf("Failed to scan row: %v", err)
			}

			batch.Rows = append(batch.Rows, row)

			// If the batch reaches the desired size, send it to the channel
			if len(batch.Rows) >= datastream.BatchSize {
				datastream.BatchChan <- batch
				batch = data.Batch{Rows: make([][]any, 0, datastream.BatchSize)}
			}
		}

		// Send any remaining rows in the batch
		if len(batch.Rows) > 0 {
			datastream.BatchChan <- batch
		}
	}()

	readWg.Wait()

	// Close the batch channel and wait for the processing to complete
	close(datastream.BatchChan)
	writeWg.Wait()

	if err := writer.Flush(); err != nil {
		log.Fatalf("Failed to flush writer: %v", err)
	}

}
