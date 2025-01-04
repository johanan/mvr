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

func (cw *CSVDataWriter) ProcessRow(row []any) ([]string, error) {
	stringRow := make([]string, len(row))
	for i, col := range row {
		dest := cw.datastream.DestColumns[i]
		if col == nil {
			stringRow[i] = "NULL"
			continue
		}

		switch dest.DatabaseType {
		case "TIMESTAMP":
			if t, ok := col.(time.Time); ok {
				stringRow[i] = t.Format(data.RFC3339NanoNoTZ)
			} else {
				stringRow[i] = cast.ToString(col)
			}
		case "TIMESTAMPTZ":
			if t, ok := col.(time.Time); ok {
				stringRow[i] = t.Format(time.RFC3339Nano)
			} else {
				stringRow[i] = cast.ToString(col)
			}
		case "NUMERIC":
			// get type of value
			switch v := col.(type) {
			case float64:
				stringRow[i] = cast.ToString(v)
			case decimal.Decimal:
				stringRow[i] = v.StringFixed(int32(dest.Scale))
			case pgtype.Numeric:
				j, err := json.Marshal(v)
				if err != nil {
					log.Fatalf("Failed to marshal pgtype.Numeric: %v", err)
				}
				stringRow[i] = string(j)
			default:
				stringRow[i] = cast.ToString(v)
			}
		case "UUID":
			switch v := col.(type) {
			case [16]uint8:
				uuidValue, err := uuid.FromBytes(v[:])
				if err != nil {
					log.Fatalf("Failed to create UUID from bytes: %v", err)
				}
				stringRow[i] = uuidValue.String()
			case []uint8:
				// Handle slice type
				uuidValue, err := uuid.FromBytes(v)
				if err != nil {
					log.Fatalf("Failed to create UUID from []uint8: %v", err)
				}
				stringRow[i] = uuidValue.String()
			default:
				stringRow[i] = cast.ToString(col)
			}
		case "JSON", "JSONB", "_TEXT":
			// if this is a string unmarshall first then remarshal
			var u interface{}
			if v, ok := col.(string); ok {
				if err := json.Unmarshal([]byte(v), &u); err != nil {
					log.Fatalf("Failed to unmarshal JSON: %v", err)
				}
			} else {
				u = col
			}

			j, err := json.Marshal(u)
			if err != nil {
				log.Fatalf("Failed to marshal JSON: %v", err)
			}
			stringRow[i] = string(j)
		default:
			stringRow[i] = cast.ToString(col)
		}
	}

	return stringRow, nil
}

func (cw *CSVDataWriter) Flush() error {
	cw.writer.Flush()
	return cw.writer.Error()
}

func (cw *CSVDataWriter) Close() error {
	cw.Flush()
	return nil
}

func (cw *CSVDataWriter) CreateBatchWriter() data.BatchWriter {
	buffer := make([][]string, 0, cw.datastream.BatchSize)
	return &CSVBatchWriter{dataWriter: cw, buffer: buffer}
}

func NewCSVDataWriter(datastream *data.DataStream, writer io.Writer) *CSVDataWriter {
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

	return &CSVDataWriter{datastream: datastream, writer: w, mux: &sync.Mutex{}}
}
