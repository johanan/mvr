package file

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/johanan/mvr/data"
)

func BuildFullPath(parsed *url.URL, config *data.StreamConfig) (string, error) {
	switch parsed.Scheme {
	case "stdout":
		return "stdout", nil
	case "file":
		if config.Filename == "" {
			return "", fmt.Errorf("filename in StreamConfig cannot be nil")
		}

		if strings.TrimSpace(config.Filename) == "" {
			return "", fmt.Errorf("filename in StreamConfig cannot be empty")
		}

		filePath := filepath.Join(parsed.Path, config.Filename)
		return filePath, nil
	default:
		return "", fmt.Errorf("invalid scheme: %s", parsed.Scheme)
	}
}

func GetIo(filePath string) (io *bufio.Writer, err error) {
	switch filePath {
	case "stdout":
		io = bufio.NewWriter(os.Stdout)
	default:
		if filePath == "" {
			return nil, fmt.Errorf("filename in StreamConfig cannot be nil")
		}

		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("error creating directory: %s", err)
		}

		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				return nil, fmt.Errorf("error removing file: %s", err)
			}
		}

		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
		}

		io = bufio.NewWriter(file)
	}
	return io, err
}

func AddCSV(ds *data.DataStream, writer io.Writer) *CSVDataWriter {
	return NewCSVDataWriter(ds, writer)
}

func AddJSONL(ds *data.DataStream, writer io.Writer) *JSONLWriter {
	return &JSONLWriter{datastream: ds, writer: writer}
}

func AddParquet(ds *data.DataStream, writer io.Writer) *ParquetDataWriter {
	return NewParquetDataWriter(ds, writer)
}
