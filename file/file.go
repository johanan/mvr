package file

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
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
		progressbar.OptionSetWidth(1),
		progressbar.OptionThrottle(1*time.Second),
		progressbar.OptionShowCount(),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionSetRenderBlankState(false),
		progressbar.OptionSetSpinnerChangeInterval(1*time.Second),
	)
}

func BuildFullPath(parsed *url.URL, filename string) (*url.URL, error) {
	switch parsed.Scheme {
	case "stdout":
		return parsed, nil
	default:
		if filename == "" {
			return nil, fmt.Errorf("filename in StreamConfig cannot be nil")
		}

		if strings.TrimSpace(filename) == "" {
			return nil, fmt.Errorf("filename in StreamConfig cannot be empty")
		}
		newURL := *parsed

		filePath, err := url.JoinPath(newURL.Path, filename)
		if err != nil {
			return nil, fmt.Errorf("error joining path: %s", err)
		}
		newURL.Path = filePath
		return &newURL, nil
	}
}

type BufferedWriter struct {
	bufWriter *bufio.Writer
	resources []io.WriteCloser
	open      bool
}

func NewBufferedWriter(writer io.Writer, closers ...io.WriteCloser) *BufferedWriter {
	return &BufferedWriter{
		bufWriter: bufio.NewWriter(writer),
		resources: closers,
		open:      true,
	}
}

func (b *BufferedWriter) Write(p []byte) (n int, err error) {
	return b.bufWriter.Write(p)
}

func (b *BufferedWriter) Close() error {
	var closeErr error
	if b.open {
		b.open = false

		if err := b.bufWriter.Flush(); err != nil {
			closeErr = err
		}

		for _, resource := range b.resources {
			if err := resource.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}
	}

	return closeErr
}

func GetIo(ctx context.Context, counting io.WriteCloser, filePath *url.URL) (io.WriteCloser, error) {
	var buf io.WriteCloser
	switch filePath.Scheme {
	case "stdout":
		f := io.MultiWriter(os.Stdout, counting)
		buf = NewBufferedWriter(f, os.Stdout, counting)
	case "azurite":
		blobConfig, err := ParseAzurite(filePath)
		if err != nil {
			return nil, fmt.Errorf("error parsing azurite url: %s", err)
		}
		azuriteBlob, err := blobConfig.GetWriter(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting azurite writer: %s", err)
		}
		f := io.MultiWriter(azuriteBlob, counting)
		buf = NewBufferedWriter(f, azuriteBlob)
	case "azure", "https":
		blobConfig, err := ParseAzureBlobURL(filePath)
		if err != nil {
			return nil, fmt.Errorf("error parsing azure blob url: %s", err)
		}
		azureBlob, err := blobConfig.GetWriter(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting azure blob writer: %s", err)
		}
		f := io.MultiWriter(azureBlob, counting)
		buf = NewBufferedWriter(f, azureBlob)
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
		buf = NewBufferedWriter(f, file, counting)
	}
	return buf, nil
}

func GetPathAndIO(ctx context.Context, path *url.URL, counter io.WriteCloser, compression, format string) (io.WriteCloser, error) {

	bufWriter, err := GetIo(ctx, counter, path)
	if err != nil {
		return nil, err
	}

	compressedWriter, err := CreateCompressedWriter(bufWriter, compression, format)
	if err != nil {
		return nil, err
	}

	return compressedWriter, nil
}

func AddCSV(ds *data.DataStream, writer io.WriteCloser) *CSVDataWriter {
	return NewCSVDataWriter(ds, writer)
}

func AddJSONL(ds *data.DataStream, writer io.WriteCloser) *JSONLWriter {
	return NewJSONLWriter(ds, writer)
}

func AddParquet(ds *data.DataStream, writer io.WriteCloser) *ParquetDataWriter {
	return NewParquetDataWriter(ds, writer)
}

func AddFileWriter(format string, ds *data.DataStream, writer io.WriteCloser) (data.DataWriter, error) {
	var dataWriter data.DataWriter
	switch strings.ToLower(format) {
	case "jsonl":
		dataWriter = AddJSONL(ds, writer)
	case "csv":
		dataWriter = AddCSV(ds, writer)
	case "parquet":
		dataWriter = AddParquet(ds, writer)
	case "arrow":
		dataWriter = NewArrowDataWriter(ds, writer)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
	return dataWriter, nil
}

func CreateCompressedWriter(bufWriter io.WriteCloser, compressionType string, format string) (io.WriteCloser, error) {
	if format == "parquet" {
		return bufWriter, nil
	}

	if compressionType == "gzip" {
		gzipWriter := gzip.NewWriter(bufWriter)
		return &gzipWriteCloser{gzipWriter: gzipWriter, bufferedWriter: bufWriter}, nil
	}

	return bufWriter, nil
}

type gzipWriteCloser struct {
	gzipWriter     *gzip.Writer
	bufferedWriter io.WriteCloser
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

	if err := w.bufferedWriter.Close(); err != nil {
		return err
	}

	return nil
}
