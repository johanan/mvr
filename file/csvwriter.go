package file

import (
	"encoding/csv"
	"io"
	"log"
	"time"

	"github.com/johanan/mvr/data"
	"github.com/spf13/cast"
)

type CSVDataWriter struct {
	datastream *data.DataStream
	writer     *csv.Writer
}

func (cw *CSVDataWriter) WriteRow(row []any) error {
	stringRow := make([]string, len(row))
	for i, col := range row {
		switch cw.datastream.Columns[i].ScanType {
		case "Time":
			// Check if the db type has tz info or not
			if t, ok := col.(time.Time); ok {
				if cw.datastream.Columns[i].DatabaseType == "TIMESTAMP" {
					stringRow[i] = t.Format(data.RFC3339NanoNoTZ)
				} else if cw.datastream.Columns[i].DatabaseType == "TIMESTAMPTZ" {
					stringRow[i] = t.Format(time.RFC3339Nano)
				}
			} else {
				stringRow[i] = cast.ToString(col)
			}
		default:
			stringRow[i] = cast.ToString(col)
		}
	}
	return cw.writer.Write(stringRow)
}

func (cw *CSVDataWriter) Flush() error {
	cw.writer.Flush()
	return cw.writer.Error()
}

func (cw *CSVDataWriter) Close() error {
	cw.Flush()
	return nil
}

func NewCSVDataWriter(datastream *data.DataStream, writer io.Writer) *CSVDataWriter {
	w := csv.NewWriter(writer)
	w.Comma = ','
	header := make([]string, 0, len(datastream.Columns))
	for _, col := range datastream.Columns {
		header = append(header, col.Name)
	}
	err := w.Write(header)
	if err != nil {
		log.Fatalf("Failed to write header: %v", err)
	}
	return &CSVDataWriter{datastream: datastream, writer: w}
}
