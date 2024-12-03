package file

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
}

func (cw *CSVDataWriter) WriteRow(row []any) error {
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
				fmt.Printf("Unknown type for NUMERIC: %T\n", v)
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
			default:
				stringRow[i] = cast.ToString(col)
			}
		case "JSON", "JSONB", "_TEXT":
			j, err := json.Marshal(col)
			if err != nil {
				log.Fatalf("Failed to marshal JSON: %v", err)
			}
			stringRow[i] = string(j)
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
	return &CSVDataWriter{datastream: datastream, writer: w}
}
