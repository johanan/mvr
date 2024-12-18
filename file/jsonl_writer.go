package file

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/johanan/mvr/data"
	"github.com/shopspring/decimal"
)

type JSONLWriter struct {
	datastream *data.DataStream
	writer     io.Writer
}

func (w *JSONLWriter) WriteRow(row []any) error {
	// Create a map of column names to values
	jsonObject := make(map[string]any, len(w.datastream.DestColumns))
	for i, col := range w.datastream.DestColumns {
		switch col.DatabaseType {
		case "TIMESTAMP":
			if t, ok := row[i].(time.Time); ok {
				jsonObject[col.Name] = t.Format(data.RFC3339NanoNoTZ)
			} else {
				jsonObject[col.Name] = row[i]
			}
		case "UUID":
			switch v := row[i].(type) {
			case [16]uint8:
				uuidValue, err := uuid.FromBytes(v[:])
				if err != nil {
					return err
				}
				jsonObject[col.Name] = uuidValue.String()
			case []uint8:
				uuidValue, err := uuid.FromBytes(v[:])
				if err != nil {
					return err
				}
				jsonObject[col.Name] = uuidValue.String()
			default:
				jsonObject[col.Name] = row[i]
			}
		case "NUMERIC":
			switch v := row[i].(type) {
			case decimal.Decimal:
				jsonObject[col.Name] = v.StringFixed(int32(col.Scale))
			case []uint8:
				jsonObject[col.Name] = string(v)
			default:
				jsonObject[col.Name] = row[i]
			}
		case "JSON":
			switch v := row[i].(type) {
			case string:
				var u interface{}
				if err := json.Unmarshal([]byte(v), &u); err != nil {
					return fmt.Errorf("failed to unmarshal JSON: %v", err)
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

func (w *JSONLWriter) Close() error {
	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
