package file

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johanan/mvr/data"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

type CSVDataWriter struct {
	datastream *data.DataStream
	writer     *csv.Writer
	resource   io.WriteCloser
	mux        *sync.Mutex
}

type CSVBatchWriter struct {
	dataWriter *CSVDataWriter
	buffer     [][]string
}

func (cb *CSVBatchWriter) WriteBatch(batch data.Batch) error {
	for _, row := range batch.Rows {
		processed, err := cb.dataWriter.ProcessRow(row)
		if err != nil {
			return err
		}
		cb.buffer = append(cb.buffer, processed)
	}

	cb.dataWriter.mux.Lock()
	defer cb.dataWriter.mux.Unlock()
	for _, row := range cb.buffer {
		if err := cb.dataWriter.writer.Write(row); err != nil {
			return err
		}
	}
	cb.dataWriter.writer.Flush()
	cb.buffer = cb.buffer[:0]

	return nil
}

func ValueToString(value any, col data.Column) (string, error) {
	switch col.Type {
	case "TIMESTAMP":
		if t, ok := value.(time.Time); ok {
			return t.Format(data.RFC3339NanoNoTZ), nil
		} else {
			return cast.ToString(value), nil
		}
	case "TIMESTAMPTZ":
		if t, ok := value.(time.Time); ok {
			return t.Format(time.RFC3339Nano), nil
		} else {
			return cast.ToString(value), nil
		}
	case "JSON", "JSONB", "_TEXT":
		// if this is a string unmarshall first then remarshal
		var u interface{}
		if v, ok := value.(string); ok {
			if err := json.Unmarshal([]byte(v), &u); err != nil {
				return "", err
			}
		} else {
			u = value
		}

		j, err := json.Marshal(u)
		if err != nil {
			return "", err
		}
		return string(j), nil
	default:
		switch v := value.(type) {
		// already a string
		case string:
			return v, nil
		// UUID cases
		case [16]uint8:
			uuidValue, err := uuid.FromBytes(v[:])
			if err != nil {
				return "", err
			}
			return uuidValue.String(), nil
		case uuid.UUID:
			return v.String(), nil
		case []uint8:
			// make sure this is not a decimal byte array
			// MS SQL makes these byte arrays of the string
			if col.Type == "NUMERIC" {
				return cast.ToString(value), nil
			}
			// continue with UUID logic
			uuidValue, err := uuid.FromBytes(v)
			if err != nil {
				log.Fatalf("Failed to create UUID from []uint8: %v", err)
			}
			return uuidValue.String(), nil
		// float and decimal cases
		case float64:
			return cast.ToString(v), nil
		case decimal.Decimal:
			return v.StringFixed(int32(col.Scale)), nil
		case pgtype.Numeric:
			j, err := json.Marshal(v)
			if err != nil {
				log.Fatalf("Failed to marshal pgtype.Numeric: %v", err)
			}
			return string(j), nil
		// just cast it at this point
		default:
			return cast.ToString(v), nil
		}
	}
}

func (cw *CSVDataWriter) ProcessRow(row []any) ([]string, error) {
	stringRow := make([]string, len(row))
	for i, col := range row {
		dest := cw.datastream.DestColumns[i]
		if col == nil {
			stringRow[i] = "NULL"
			continue
		}

		value, err := ValueToString(col, dest)
		if err != nil {
			return nil, err
		}
		stringRow[i] = value
	}

	return stringRow, nil
}

func (cw *CSVDataWriter) Flush() error {
	cw.writer.Flush()
	return cw.writer.Error()
}

func (cw *CSVDataWriter) Close() error {
	cw.Flush()
	cw.resource.Close()
	return nil
}

func (cw *CSVDataWriter) CreateBatchWriter() data.BatchWriter {
	buffer := make([][]string, 0, cw.datastream.BatchSize)
	return &CSVBatchWriter{dataWriter: cw, buffer: buffer}
}

func NewCSVDataWriter(datastream *data.DataStream, writer io.WriteCloser) *CSVDataWriter {
	w := csv.NewWriter(writer)
	w.Comma = ','
	if datastream == nil {
		log.Fatalf("Datastream is nil")
	}
	header := make([]string, 0, len(datastream.Columns))
	for _, col := range datastream.Columns {
		header = append(header, col.Name)
	}
	err := w.Write(header)
	if err != nil {
		log.Fatalf("Failed to write header: %v", err)
	}

	return &CSVDataWriter{datastream: datastream, writer: w, mux: &sync.Mutex{}, resource: writer}
}
