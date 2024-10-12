package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"net/url"
	"os"
	"runtime"

	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/johanan/mvr/file"
)

func main() {
	connStr := os.Getenv("MVR_SOURCE")
	destConnStr := os.Getenv("MVR_DEST")

	streamConfig := &core.StreamConfig{StreamName: "public.users", Format: "csv"}

	config := core.NewConfig(connStr, destConnStr, streamConfig)
	numCores := runtime.NumCPU()
	runtime.GOMAXPROCS(numCores)
	execConfig := core.NewExecutionConfig(config, numCores)

	// wraps context and config
	task := core.NewTask(*execConfig)
	datastream, db := database.CreateDataStream(task.ExecConfig.Config.SourceConn.ParsedUrl, task.ExecConfig.ReaderOptions.Sql)
	task.ExecConfig.ReaderConn = db

	// just a string for now
	compressedWriter, err := CreateCompressedWriter(task.ExecConfig.Config.DestConn.ParsedUrl, "none")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := compressedWriter.Close(); err != nil {
			log.Printf("Error closing compressed writer: %v\n", err)
		}
	}()

	writer := file.AddJSONL(datastream, compressedWriter)
	execute(task, datastream, writer)
}

func CreateCompressedWriter(outputURL *url.URL, compressionType string) (io.WriteCloser, error) {
	bufWriter, err := file.GetIo(outputURL)
	if err != nil {
		return nil, err
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

func execute(task *core.Task, datastream *data.DataStream, writer data.DataWriter) {
	var readWg = task.ExecConfig.ReaderWg
	var writeWg = task.ExecConfig.WriterWg

	columns := datastream.Columns

	for i := 0; i < task.ExecConfig.Concurrency; i++ {
		writeWg.Add(1)
		go datastream.BatchesToWriter(writeWg, writer)
	}

	db := task.ExecConfig.ReaderConn
	defer db.Close()
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

	if closer, ok := writer.(io.WriteCloser); ok {
		if err := closer.Close(); err != nil {
			log.Fatalf("Failed to close writer: %v", err)
		}
	}

}
