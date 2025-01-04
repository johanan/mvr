package file

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/johanan/mvr/data"
	"github.com/schollz/progressbar/v3"
)

func NewProgressBar() *progressbar.ProgressBar {
	return progressbar.NewOptions64(
		-1,
		progressbar.OptionSetDescription("Writing data"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowTotalBytes(true),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(1*time.Second),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(false),
		progressbar.OptionSetSpinnerChangeInterval(1*time.Second),
	)
}

func BuildFullPath(parsed *url.URL, filename string) (*url.URL, error) {
	switch parsed.Scheme {
	case "stdout":
		return parsed, nil
	case "file":
		if filename == "" {
			return nil, fmt.Errorf("filename in StreamConfig cannot be nil")
		}

		if strings.TrimSpace(filename) == "" {
			return nil, fmt.Errorf("filename in StreamConfig cannot be empty")
		}

		filePath, err := url.JoinPath(parsed.Path, filename)
		if err != nil {
			return nil, fmt.Errorf("error joining path: %s", err)
		}
		return &url.URL{Scheme: parsed.Scheme, Path: filePath}, nil
	default:
		return nil, fmt.Errorf("invalid scheme: %s", parsed.Scheme)
	}
}

func GetIo(counting io.Writer, filePath *url.URL) (*bufio.Writer, error) {
	var buf *bufio.Writer
	switch filePath.Scheme {
	case "stdout":
		f := io.MultiWriter(os.Stdout, counting)
		buf = bufio.NewWriter(f)
	default:
		if filePath == nil {
			return nil, fmt.Errorf("filename in StreamConfig cannot be nil")
		}

		dir := filepath.Dir(filePath.Path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("error creating directory: %s", err)
		}

		if _, err := os.Stat(filePath.Path); err == nil {
			if err := os.Remove(filePath.Path); err != nil {
				return nil, fmt.Errorf("error removing file: %s", err)
			}
		}

		file, err := os.OpenFile(filePath.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
		}
		f := io.MultiWriter(file, counting)
		buf = bufio.NewWriter(f)
	}
	return buf, nil
}

func GetPathAndIO(parsed *url.URL, counter io.Writer, filename, compression, format string) (*url.URL, io.WriteCloser, error) {
	path, err := BuildFullPath(parsed, filename)
	if err != nil {
		return nil, nil, err
	}

	bufWriter, err := GetIo(counter, path)
	if err != nil {
		return nil, nil, err
	}

	compressedWriter, err := CreateCompressedWriter(bufWriter, compression, format)
	if err != nil {
		return nil, nil, err
	}

	return path, compressedWriter, nil
}

func AddCSV(ds *data.DataStream, writer io.Writer) *CSVDataWriter {
	return NewCSVDataWriter(ds, writer)
}

func AddJSONL(ds *data.DataStream, writer io.Writer) *JSONLWriter {
	return NewJSONLWriter(ds, writer)
}

func AddParquet(ds *data.DataStream, writer io.Writer) *ParquetDataWriter {
	return NewParquetDataWriter(ds, writer)
}

func AddFileWriter(format string, ds *data.DataStream, writer io.Writer) (data.DataWriter, error) {
	var dataWriter data.DataWriter
	switch strings.ToLower(format) {
	case "jsonl":
		dataWriter = AddJSONL(ds, writer)
	case "csv":
		dataWriter = AddCSV(ds, writer)
	case "parquet":
		dataWriter = AddParquet(ds, writer)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
	return dataWriter, nil
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
