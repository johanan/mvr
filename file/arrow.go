package file

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/johanan/mvr/data"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

type ArrowDataWriter struct {
	datastream *data.DataStream
	writer     *ipc.Writer
	schema     *arrow.Schema
	alloc      memory.Allocator
	mux        *sync.Mutex
}

type ArrowBatchWriter struct {
	dataWriter    *ArrowDataWriter
	builders      []array.Builder
	recordBuilder *array.RecordBuilder
}

func NewArrowDataWriter(datastream *data.DataStream, w io.Writer) *ArrowDataWriter {
	alloc := memory.NewGoAllocator()

	fields := make([]arrow.Field, len(datastream.DestColumns))
	for i, col := range datastream.DestColumns {
		// Add column metadata to preserve type information
		metadata := arrow.NewMetadata(
			[]string{"original_type", "precision", "scale", "length"},
			[]string{col.Type,
				cast.ToString(col.Precision),
				cast.ToString(col.Scale),
				cast.ToString(col.Length)},
		)

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     mapToArrowDataType(col),
			Nullable: true,
			Metadata: metadata,
		}
	}

	schema := arrow.NewSchema(fields, nil)
	writer := ipc.NewWriter(w, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	return &ArrowDataWriter{
		datastream: datastream,
		writer:     writer,
		schema:     schema,
		alloc:      alloc,
		mux:        &sync.Mutex{},
	}

}

func (aw *ArrowDataWriter) CreateBatchWriter() data.BatchWriter {
	recordBuilder := array.NewRecordBuilder(aw.alloc, aw.schema)

	return &ArrowBatchWriter{
		dataWriter:    aw,
		recordBuilder: recordBuilder,
		builders:      recordBuilder.Fields(),
	}
}

func (ab *ArrowBatchWriter) WriteBatch(batch data.Batch) error {
	// Acquire lock to ensure thread safety
	ab.dataWriter.mux.Lock()
	defer ab.dataWriter.mux.Unlock()

	// Reset for new batch
	for _, builder := range ab.builders {
		builder.Reserve(len(batch.Rows))
	}

	// Process each row
	for _, row := range batch.Rows {
		for i, val := range row {
			col := ab.dataWriter.datastream.DestColumns[i]
			if err := appendToBuilder(ab.builders[i], val, col); err != nil {
				return err
			}
		}
	}

	// Create Arrow record and write it
	record := ab.recordBuilder.NewRecord()
	defer record.Release()

	if err := ab.dataWriter.writer.Write(record); err != nil {
		return err
	}

	// Reset builders for next batch
	ab.recordBuilder.Reserve(ab.dataWriter.datastream.BatchSize)

	return nil
}

func (aw *ArrowDataWriter) Flush() error {
	return nil
}

func (aw *ArrowDataWriter) Close() error {
	return aw.writer.Close()
}

func mapToArrowDataType(col data.Column) arrow.DataType {
	aliased := data.TypeAlias(col.Type)

	switch aliased {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "REAL":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case "UUID":
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case "TIMESTAMPTZ":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case "NUMERIC":
		if col.Precision <= 0 {
			return arrow.PrimitiveTypes.Float64
		}
		return &arrow.Decimal128Type{Precision: int32(col.Precision), Scale: int32(col.Scale)}
	case "VARCHAR", "TEXT", "_TEXT":
		return arrow.BinaryTypes.String
	case "JSONB", "JSON":
		// Use String type with metadata to indicate it's JSON
		return &arrow.StringType{}
	default:
		return arrow.BinaryTypes.String
	}
}

func appendToBuilder(builder array.Builder, val interface{}, col data.Column) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		boolVal, ok := val.(bool)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.Append(boolVal)

	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			b.Append(v)
		case int32:
			b.Append(int16(v))
		case int64:
			b.Append(int16(v))
		default:
			b.Append(cast.ToInt16(val))
		}

	case *array.Int32Builder:
		switch v := val.(type) {
		case *big.Int:
			b.Append(int32(v.Int64()))
		case int32:
			b.Append(v)
		default:
			b.Append(cast.ToInt32(val))
		}

	case *array.Int64Builder:
		switch v := val.(type) {
		case *big.Int:
			b.Append(v.Int64())
		case int32:
			b.Append(int64(v))
		case int64:
			b.Append(v)
		case decimal.Decimal:
			b.Append(v.IntPart())
		default:
			b.Append(cast.ToInt64(val))
		}

	case *array.Float32Builder:
		switch v := val.(type) {
		case *big.Float:
			f32, _ := v.Float32()
			b.Append(f32)
		default:
			b.Append(cast.ToFloat32(val))
		}

	case *array.Float64Builder:
		switch v := val.(type) {
		case *big.Float:
			f64, _ := v.Float64()
			b.Append(f64)
		case decimal.Decimal:
			f64, _ := v.Float64()
			b.Append(f64)
		default:
			b.Append(cast.ToFloat64(val))
		}

	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		default:
			if col.Type == "JSONB" || col.Type == "JSON" {
				j, err := json.Marshal(val)
				if err != nil {
					return err
				}
				b.Append(string(j))
			} else {
				// Use your existing ValueToString helper
				str, err := ValueToString(val, col)
				if err != nil {
					return err
				}
				b.Append(str)
			}
		}

	case *array.FixedSizeBinaryBuilder:
		switch v := val.(type) {
		case string:
			if col.Type == "UUID" {
				uuidVal, err := uuid.Parse(v)
				if err != nil {
					return err
				}
				b.Append(uuidVal[:])
			}
		case []byte:
			b.Append(v)
		case [16]uint8:
			b.Append(v[:])
		case uuid.UUID:
			b.Append(v[:])
		}

	case *array.Date32Builder:
		dateValue, ok := val.(time.Time)
		if !ok {
			b.AppendNull()
			return nil
		}
		days := int32(dateValue.Sub(epochDate).Hours() / 24)
		b.Append(arrow.Date32(days))

	case *array.TimestampBuilder:
		tsValue, ok := val.(time.Time)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.Append(arrow.Timestamp(tsValue.UnixMicro()))

	case *array.Decimal128Builder:
		switch v := val.(type) {
		case decimal.Decimal:
			// Convert decimal.Decimal to arrow.Decimal128
			decimalVal, err := decimalToArrowDecimal128(v, int32(col.Precision), int32(col.Scale))
			if err != nil {
				return err
			}
			b.Append(decimalVal)
		case *big.Float:
			// Convert big.Float to arrow.Decimal128
			decStr := v.Text('f', int(col.Scale))
			decimalVal, err := decimal128.FromString(decStr, int32(col.Precision), int32(col.Scale))
			if err != nil {
				return err
			}
			b.Append(decimalVal)
		case string:
			decimalVal, err := decimal128.FromString(v, int32(col.Precision), int32(col.Scale))
			if err != nil {
				return err
			}
			b.Append(decimalVal)
		default:
			// Try to convert to string first
			str, err := ValueToString(val, col)
			if err != nil {
				return err
			}
			decimalVal, err := decimal128.FromString(str, int32(col.Precision), int32(col.Scale))
			if err != nil {
				return err
			}
			b.Append(decimalVal)
		}

	default:
		return fmt.Errorf("unsupported builder type %T for value %v", builder, val)
	}

	return nil
}

func decimalToArrowDecimal128(d decimal.Decimal, precision, scale int32) (decimal128.Num, error) {
	// Scale the decimal to the right precision
	scaledDec := d.Mul(decimal.NewFromInt(10).Pow(decimal.NewFromInt32(scale)))

	// Convert to big.Int
	bi := scaledDec.BigInt()

	// Convert to decimal128.Num
	return decimal128.FromBigInt(bi), nil
}
