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

func GetIo(parsed *url.URL, config *data.StreamConfig) (io *bufio.Writer, err error) {
	switch parsed.Scheme {
	case "stdout":
		io = bufio.NewWriter(os.Stdout)
	case "file":
		if config.Filename == nil {
			return nil, fmt.Errorf("filename in StreamConfig cannot be nil")
		}

		if strings.TrimSpace(*config.Filename) == "" {
			return nil, fmt.Errorf("filename in StreamConfig cannot be empty")
		}

		filePath := filepath.Join(parsed.Path, *config.Filename)

		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("error creating directory: %s", err)
		}

		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
		}

		io = bufio.NewWriter(file)
	default:
		io = nil
		err = fmt.Errorf("invalid scheme: %s", parsed.Scheme)
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
