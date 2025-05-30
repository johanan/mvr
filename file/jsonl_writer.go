package file

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/johanan/mvr/data"
)

type JSONLWriter struct {
	datastream *data.DataStream
	writer     io.Writer
	mux        *sync.Mutex
}

type JSONLBatchWriter struct {
	dataWriter *JSONLWriter
	buffer     [][]byte
}

func NewJSONLWriter(ds *data.DataStream, writer io.Writer) *JSONLWriter {
	return &JSONLWriter{datastream: ds, writer: writer, mux: &sync.Mutex{}}
}

func (w *JSONLWriter) CreateBatchWriter() data.BatchWriter {
	buffer := make([][]byte, 0, w.datastream.BatchSize)
	return &JSONLBatchWriter{dataWriter: w, buffer: buffer}
}

func (bw *JSONLBatchWriter) WriteBatch(batch data.Batch) error {
	for _, row := range batch.Rows {
		jsonLine, err := bw.dataWriter.ProcessRow(row)
		if err != nil {
			return err
		}
		bw.buffer = append(bw.buffer, jsonLine)
	}

	bw.dataWriter.mux.Lock()
	defer bw.dataWriter.mux.Unlock()
	for _, line := range bw.buffer {
		line = append(line, '\n')
		if _, err := bw.dataWriter.writer.Write(line); err != nil {
			return err
		}
	}

	bw.dataWriter.Flush()
	bw.buffer = bw.buffer[:0]

	return nil
}

func (w *JSONLWriter) ProcessRow(row []any) ([]byte, error) {
	// Create a map of column names to values
	jsonObject := make(map[string]any, len(w.datastream.DestColumns))
	for i, col := range w.datastream.DestColumns {
		switch col.Type {
		case "TIMESTAMP", "TIMESTAMPTZ", "UUID", "NUMERIC":
			if row[i] == nil {
				jsonObject[col.Name] = nil
				continue
			}
			value, err := ValueToString(row[i], col)
			if err != nil {
				return nil, err
			}
			jsonObject[col.Name] = value
		case "JSON", "JSONB":
			switch v := row[i].(type) {
			case string:
				var u interface{}
				// Unmarshal the JSON string into an interface so it's not double JSON encoded
				if err := json.Unmarshal([]byte(v), &u); err != nil {
					return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
				}
				jsonObject[col.Name] = u
			default:
				jsonObject[col.Name] = row[i]
			}
		default:
			jsonObject[col.Name] = row[i]
		}
	}

	// Marshal the map to JSON
	line, err := json.Marshal(jsonObject)
	if err != nil {
		return nil, err
	}

	return line, err
}

func (w *JSONLWriter) Flush() error {
	if flusher, ok := w.writer.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

func (w *JSONLWriter) Close() error {
	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
