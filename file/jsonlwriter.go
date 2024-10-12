package file

import (
	"encoding/json"
	"io"

	"github.com/johanan/mvr/data"
)

type JSONLWriter struct {
	datastream *data.DataStream
	writer     io.Writer
}

func (w *JSONLWriter) WriteRow(row []any) error {
	// Create a map of column names to values
	jsonObject := make(map[string]any, len(w.datastream.Columns))
	for i, col := range w.datastream.Columns {
		jsonObject[col.Name] = row[i]
	}

	// Marshal the map to JSON
	line, err := json.Marshal(jsonObject)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	_, err = w.writer.Write(line)
	return err
}

func (w *JSONLWriter) Flush() error {
	if flusher, ok := w.writer.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}
