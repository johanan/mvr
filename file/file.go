package file

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/johanan/mvr/data"
)

func GetIo(parsed *url.URL) (io *bufio.Writer, err error) {
	switch parsed.Scheme {
	case "stdout":
		io = bufio.NewWriter(os.Stdout)
	default:
		io = nil
		err = fmt.Errorf("invalid scheme: %s", parsed.Scheme)
	}
	return io, err
}

func AddCSV(ds *data.DataStream, writer io.Writer) data.DataWriter {
	return NewCSVDataWriter(ds, writer)
}

func AddJSONL(ds *data.DataStream, writer io.Writer) *JSONLWriter {
	return &JSONLWriter{datastream: ds, writer: writer}
}
