package file

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/google/uuid"
	"github.com/johanan/mvr/data"

	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

type ColumnMetadata struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Len  int32  `json:"len"`
}

type ParquetDataWriter struct {
	datastream *data.DataStream
	writer     *file.Writer
	mux        sync.Mutex
}

type ParquetBatchWriter struct {
	dataWriter       *ParquetDataWriter
	columnBuffers    []interface{}
	definitionLevels [][]int16
	scaleFactors     map[int]*big.Float
}

type ParquetDecimal struct {
	Type         DecimalType // Discriminator to indicate the active type
	Int32Val     int32
	Int64Val     int64
	ByteArrayVal []byte
}

type DecimalType int

const (
	UnknownDecimal DecimalType = iota
	Int32Decimal
	Int64Decimal
	ByteArrayDecimal
)

type MappedType struct {
	ParquetType parquet.Type
	LogicalType schema.LogicalType
	GoType      reflect.Type
}

// Numeric is it's own lookup
var parquetTypeMap = map[string]MappedType{
	"BOOLEAN":     {parquet.Types.Boolean, nil, reflect.TypeOf(bool(false))},
	"SMALLINT":    {parquet.Types.Int32, nil, reflect.TypeOf(int32(0))},
	"INTEGER":     {parquet.Types.Int32, nil, reflect.TypeOf(int32(0))},
	"BIGINT":      {parquet.Types.Int64, nil, reflect.TypeOf(int64(0))},
	"REAL":        {parquet.Types.Float, nil, reflect.TypeOf(float32(0))},
	"DOUBLE":      {parquet.Types.Double, nil, reflect.TypeOf(float64(0))},
	"UUID":        {parquet.Types.FixedLenByteArray, schema.UUIDLogicalType{}, reflect.TypeOf([]byte{})},
	"DATE":        {parquet.Types.Int32, schema.DateLogicalType{}, reflect.TypeOf(int32(0))},
	"TIMESTAMP":   {parquet.Types.Int64, schema.NewTimestampLogicalType(false, schema.TimeUnitNanos), reflect.TypeOf(int64(0))},
	"TIMESTAMPTZ": {parquet.Types.Int64, schema.NewTimestampLogicalType(true, schema.TimeUnitNanos), reflect.TypeOf(int64(0))},
	"VARCHAR":     {parquet.Types.ByteArray, schema.StringLogicalType{}, reflect.TypeOf(string(""))},
	"JSONB":       {parquet.Types.ByteArray, schema.JSONLogicalType{}, reflect.TypeOf(string(""))},
	"JSON":        {parquet.Types.ByteArray, schema.JSONLogicalType{}, reflect.TypeOf(string(""))},
	"_TEXT":       {parquet.Types.ByteArray, schema.StringLogicalType{}, reflect.TypeOf(string(""))},
}

func numericLookup(precision int, scale int) MappedType {
	dec := schema.NewDecimalLogicalType(int32(precision), int32(scale))
	if precision <= 9 {
		return MappedType{parquet.Types.Int32, dec, reflect.TypeOf(int32(0))}
	} else if precision <= 18 {
		return MappedType{parquet.Types.Int64, dec, reflect.TypeOf(int64(0))}
	} else {
		return MappedType{parquet.Types.FixedLenByteArray, dec, reflect.TypeOf([]byte{})}
	}
}

func checkType(col data.Column) MappedType {
	aliased := data.TypeAlias(col.Type)
	if aliased == "NUMERIC" {
		return numericLookup(int(col.Precision), int(col.Scale))
	}
	if t, ok := parquetTypeMap[aliased]; ok {
		return t
	}
	return MappedType{parquet.Types.ByteArray, schema.StringLogicalType{}, reflect.TypeOf(string(""))}
}

func mapColumnMetadata(columns []data.Column) []ColumnMetadata {
	columnMetadata := make([]ColumnMetadata, len(columns))
	for col := range columns {
		columnMetadata[col] = ColumnMetadata{Name: columns[col].Name, Type: columns[col].Type, Len: int32(columns[col].Length)}
	}

	return columnMetadata
}

func NewParquetDataWriter(datastream *data.DataStream, ioWriter io.Writer) *ParquetDataWriter {
	columnCount := len(datastream.DestColumns)

	nodes := make([]schema.Node, columnCount)

	for i, col := range datastream.DestColumns {
		nodes[i] = buildNode(col)
	}

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, nodes, 1)
	if err != nil {
		panic(err)
	}
	s := schema.NewSchema(root)
	writerProp := parquet.WithCompression(compress.Codecs.Snappy)
	prop := parquet.NewWriterProperties(writerProp)
	fileProp := file.WithWriterProps(prop)
	writer := file.NewParquetWriter(ioWriter, s.Root(), fileProp)

	columnMetadata := mapColumnMetadata(datastream.DestColumns)
	columnMetadataJSON, err := json.Marshal(columnMetadata)
	if err == nil {
		writer.AppendKeyValueMetadata("cols", string(columnMetadataJSON))
	}
	return &ParquetDataWriter{datastream: datastream, writer: writer}
}

func (pw *ParquetDataWriter) CreateBatchWriter() data.BatchWriter {
	// allocate column buffers and definition levels
	columnCount := len(pw.datastream.DestColumns)
	columnBuffers := make([]interface{}, columnCount)
	definitionLevels := make([][]int16, columnCount)

	for i := 0; i < columnCount; i++ {
		mapped := checkType(pw.datastream.DestColumns[i])
		columnBuffers[i] = reflect.MakeSlice(reflect.SliceOf(mapped.GoType), 0, pw.datastream.BatchSize).Interface()
		definitionLevels[i] = make([]int16, 0, pw.datastream.BatchSize)
	}

	scaleFactors := make(map[int]*big.Float)

	return &ParquetBatchWriter{dataWriter: pw, columnBuffers: columnBuffers, definitionLevels: definitionLevels, scaleFactors: scaleFactors}
}

func (pb *ParquetBatchWriter) WriteBatch(batch data.Batch) error {
	for _, row := range batch.Rows {
		for i, col := range pb.dataWriter.datastream.DestColumns {
			if row[i] == nil {
				pb.definitionLevels[i] = append(pb.definitionLevels[i], 0)
				continue
			}
			// has value so set definition level to 1
			pb.definitionLevels[i] = append(pb.definitionLevels[i], 1)

			switch col.Type {
			case "SMALLINT":
				buf, ok := pb.columnBuffers[i].([]int32)
				if !ok {
					return fmt.Errorf("type assertion failed for INT2")
				}
				switch v := row[i].(type) {
				case int32:
					pb.columnBuffers[i] = append(buf, v)
				default:
					pb.columnBuffers[i] = append(buf, cast.ToInt32(row[i]))
				}
			case "INTEGER":
				buf, ok := pb.columnBuffers[i].([]int32)
				if !ok {
					return fmt.Errorf("type assertion failed for INT4")
				}
				switch v := row[i].(type) {
				case *big.Int:
					pb.columnBuffers[i] = append(buf, int32(v.Int64()))
				case int32:
					pb.columnBuffers[i] = append(buf, v)
				default:
					pb.columnBuffers[i] = append(buf, cast.ToInt32(row[i]))
				}
			case "BIGINT":
				buf, ok := pb.columnBuffers[i].([]int64)
				if !ok {
					return fmt.Errorf("type assertion failed for INT8")
				}
				switch v := row[i].(type) {
				case *big.Int:
					pb.columnBuffers[i] = append(buf, v.Int64())
				case int32:
					pb.columnBuffers[i] = append(buf, int64(v))
				case int64:
					pb.columnBuffers[i] = append(buf, v)
				case decimal.Decimal:
					pb.columnBuffers[i] = append(buf, v.IntPart())
				default:
					pb.columnBuffers[i] = append(buf, cast.ToInt64(row[i]))
				}
			case "REAL":
				buf, ok := pb.columnBuffers[i].([]float32)
				if !ok {
					return fmt.Errorf("type assertion failed for FLOAT4")
				}
				switch v := row[i].(type) {
				case *big.Float:
					f32, _ := v.Float32()
					pb.columnBuffers[i] = append(buf, f32)
				default:
					pb.columnBuffers[i] = append(buf, cast.ToFloat32(row[i]))
				}
			case "DOUBLE":
				buf, ok := pb.columnBuffers[i].([]float64)
				if !ok {
					return fmt.Errorf("type assertion failed for FLOAT8")
				}
				switch v := row[i].(type) {
				case *big.Float:
					f64, _ := v.Float64()
					pb.columnBuffers[i] = append(buf, f64)
				case decimal.Decimal:
					f64, _ := v.Float64()
					pb.columnBuffers[i] = append(buf, f64)
				default:
					pb.columnBuffers[i] = append(buf, cast.ToFloat64(row[i]))
				}
			case "JSONB", "JSON", "_TEXT":
				buf, ok := pb.columnBuffers[i].([]string)
				if !ok {
					return fmt.Errorf("type assertion failed for JSON types")
				}
				switch v := row[i].(type) {
				case string:
					pb.columnBuffers[i] = append(buf, v)
				default:
					j, err := json.Marshal(row[i])
					if err != nil {
						return fmt.Errorf("failed to marshal JSON for column %s: %v", col.Name, err)
					}
					pb.columnBuffers[i] = append(buf, string(j))
				}
			case "UUID":
				buf, ok := pb.columnBuffers[i].([][]byte)
				if !ok {
					return fmt.Errorf("type assertion failed for UUID")
				}

				switch v := row[i].(type) {
				case string:
					uuidValue, err := uuid.Parse(v)
					if err != nil {
						return fmt.Errorf("failed to parse UUID for column %s: %v", col.Name, err)
					}
					pb.columnBuffers[i] = append(buf, uuidValue[:])
				case [16]uint8:
					pb.columnBuffers[i] = append(buf, v[:])
				case uuid.UUID:
					pb.columnBuffers[i] = append(buf, v[:])
				default:
					return fmt.Errorf("expected string or UUID for UUID column %s, got %T", col.Name, v)
				}
			case "NUMERIC":
				decimalValue, err := convertToParquetDecimal(row[i], int(col.Precision), int(col.Scale), pb.scaleFactors)
				if err != nil {
					return fmt.Errorf("failed to convert DECIMAL for column %s: %v", col.Name, err)
				}
				switch decimalValue.Type {
				case Int32Decimal:
					buf, ok := pb.columnBuffers[i].([]int32)
					if !ok {
						return fmt.Errorf("type assertion failed for DECIMAL")
					}
					pb.columnBuffers[i] = append(buf, decimalValue.Int32Val)
				case Int64Decimal:
					buf, ok := pb.columnBuffers[i].([]int64)
					if !ok {
						return fmt.Errorf("type assertion failed for DECIMAL")
					}
					pb.columnBuffers[i] = append(buf, decimalValue.Int64Val)
				case ByteArrayDecimal:
					buf, ok := pb.columnBuffers[i].([][]byte)
					if !ok {
						return fmt.Errorf("type assertion failed for DECIMAL")
					}
					pb.columnBuffers[i] = append(buf, decimalValue.ByteArrayVal)
				default:
					return fmt.Errorf("unknown DECIMAL type %v", decimalValue.Type)
				}
			case "DATE":
				buf, ok := pb.columnBuffers[i].([]int32)
				if !ok {
					return fmt.Errorf("type assertion failed for DATE")
				}
				dateValue, ok := row[i].(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time for DATE column %s, got %T", col.Name, row[i])
				}
				// Convert the date to the number of days since the Unix epoch
				daysSinceEpoch := int32(dateValue.Sub(epochDate).Truncate(24*time.Hour).Hours() / 24)
				pb.columnBuffers[i] = append(buf, daysSinceEpoch)
			case "BOOLEAN":
				buf, ok := pb.columnBuffers[i].([]bool)
				if !ok {
					return fmt.Errorf("type assertion failed for BOOL")
				}
				switch v := row[i].(type) {
				case bool:
					pb.columnBuffers[i] = append(buf, v)
				default:
					return fmt.Errorf("expected bool for BOOL column %s, got %T", col.Name, row[i])
				}
			case "TIMESTAMP", "TIMESTAMPTZ":
				buf, ok := pb.columnBuffers[i].([]int64)
				if !ok {
					return fmt.Errorf("type assertion failed for TIMESTAMP")
				}
				v, ok := row[i].(time.Time)
				if !ok {
					return fmt.Errorf("expected time.Time for TIMESTAMP column %s, got %T", col.Name, row[i])
				}
				pb.columnBuffers[i] = append(buf, v.UnixNano())
			case "TEXT", "VARCHAR":
				buf, ok := pb.columnBuffers[i].([]string)
				if !ok {
					return fmt.Errorf("type assertion failed for TEXT")
				}
				// reuse the CSV logic to convert to string
				value, err := ValueToString(row[i], col)
				if err != nil {
					return fmt.Errorf("failed to convert TEXT for column %s: %v", col.Name, err)
				}
				pb.columnBuffers[i] = append(buf, value)
			default:
				// cast everything else to string
				buf, ok := pb.columnBuffers[i].([]string)
				if !ok {
					return fmt.Errorf("type assertion failed for default")
				}
				pb.columnBuffers[i] = append(buf, cast.ToString(row[i]))
			}
		}
	}

	pb.dataWriter.mux.Lock()
	err := pb.dataWriter.writeRowGroup(pb.columnBuffers, pb.definitionLevels, len(batch.Rows))
	pb.dataWriter.mux.Unlock()
	if err != nil {
		return err
	}

	// reset buffers
	for i := range pb.columnBuffers {
		switch pb.columnBuffers[i].(type) {
		case []int32:
			pb.columnBuffers[i] = pb.columnBuffers[i].([]int32)[:0]
		case []int64:
			pb.columnBuffers[i] = pb.columnBuffers[i].([]int64)[:0]
		case []float32:
			pb.columnBuffers[i] = pb.columnBuffers[i].([]float32)[:0]
		case []float64:
			pb.columnBuffers[i] = pb.columnBuffers[i].([]float64)[:0]
		case []string:
			pb.columnBuffers[i] = pb.columnBuffers[i].([]string)[:0]
		case [][]byte:
			pb.columnBuffers[i] = pb.columnBuffers[i].([][]byte)[:0]
		case []bool:
			pb.columnBuffers[i] = pb.columnBuffers[i].([]bool)[:0]
		}
		pb.definitionLevels[i] = pb.definitionLevels[i][:0]
	}

	return nil
}

func (pw *ParquetDataWriter) Flush() error {
	return nil
}

func (pw *ParquetDataWriter) Close() error {
	pw.writer.FlushWithFooter()
	return pw.writer.Close()
}

func (pw *ParquetDataWriter) writeRowGroup(columnBuffers []any, definitionLevels [][]int16, rowCount int) error {
	rg := pw.writer.AppendBufferedRowGroup()
	defer rg.Close()
	for i := range pw.datastream.DestColumns {
		rgCol, err := rg.Column(i)
		if err != nil {
			return err
		}

		switch chunker := rgCol.(type) {
		case *file.Int32ColumnChunkWriter:
			buf, ok := columnBuffers[i].([]int32)
			if !ok {
				return fmt.Errorf("type assertion failed for INT4")
			}
			_, err := chunker.WriteBatch(buf, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		case *file.Int64ColumnChunkWriter:
			buf, ok := columnBuffers[i].([]int64)
			if !ok {
				return fmt.Errorf("type assertion failed for INT8 writer")
			}
			_, err := chunker.WriteBatch(buf, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		case *file.Float32ColumnChunkWriter:
			buf, ok := columnBuffers[i].([]float32)
			if !ok {
				return fmt.Errorf("type assertion failed for FLOAT4")
			}
			_, err := chunker.WriteBatch(buf, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		case *file.Float64ColumnChunkWriter:
			buf, ok := columnBuffers[i].([]float64)
			if !ok {
				return fmt.Errorf("type assertion failed for FLOAT8")
			}
			_, err := chunker.WriteBatch(buf, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		case *file.ByteArrayColumnChunkWriter:
			buf, ok := columnBuffers[i].([]string)
			if !ok {
				return fmt.Errorf("type assertion failed for BYTE_ARRAY")
			}
			byteArray := make([]parquet.ByteArray, len(buf))
			for j := 0; j < len(buf); j++ {
				byteArray[j] = parquet.ByteArray(buf[j])
			}
			_, err := chunker.WriteBatch(byteArray, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		case *file.FixedLenByteArrayColumnChunkWriter:
			buf, ok := columnBuffers[i].([][]byte)
			if !ok {
				return fmt.Errorf("type assertion failed for FIXED_LEN_BYTE_ARRAY")
			}
			// loop over and set as parquet.FixedLenByteArray
			fixedLenByteArray := make([]parquet.FixedLenByteArray, len(buf))
			for j := 0; j < len(buf); j++ {
				fixedLenByteArray[j] = parquet.FixedLenByteArray(buf[j])
			}
			_, err := chunker.WriteBatch(fixedLenByteArray, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		case *file.BooleanColumnChunkWriter:
			buf, ok := columnBuffers[i].([]bool)
			if !ok {
				return fmt.Errorf("type assertion failed for BOOLEAN")
			}
			_, err := chunker.WriteBatch(buf, definitionLevels[i][:rowCount], nil)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("type assertion failed for default")
		}
	}
	return nil
}

func convertToParquetDecimal(value any, precision, scale int, scaleFactors map[int]*big.Float) (ParquetDecimal, error) {
	var result ParquetDecimal
	switch v := value.(type) {
	case decimal.Decimal:
		// I tried big float but it was not working
		float := v.StringFixed(int32(scale))
		return convertToParquetDecimal(float, precision, scale, scaleFactors)

	case *big.Int:
		switch {
		case precision <= 9:
			return ParquetDecimal{Type: Int32Decimal, Int32Val: int32(v.Int64())}, nil
		case precision <= 18:
			return ParquetDecimal{Type: Int64Decimal, Int64Val: v.Int64()}, nil
		default:
			byteArray, err := bigIntToFixedBytes(v, precision)
			if err != nil {
				return result, err
			}
			return ParquetDecimal{Type: ByteArrayDecimal, ByteArrayVal: byteArray}, nil
		}

	case *big.Float:
		// Convert *big.Float to unscaled int
		unscaledInt := convertBigFloatToUnscaledInt(v, scale, scaleFactors)
		switch {
		case precision <= 9:
			if !unscaledInt.IsInt64() {
				return result, fmt.Errorf("failed to convert DECIMAL to int32: %v", unscaledInt)
			}
			return ParquetDecimal{Type: Int32Decimal, Int32Val: int32(unscaledInt.Int64())}, nil

		case precision <= 18:
			if !unscaledInt.IsInt64() {
				return result, fmt.Errorf("failed to convert DECIMAL to int64: %v", unscaledInt)
			}
			return ParquetDecimal{Type: Int64Decimal, Int64Val: unscaledInt.Int64()}, nil

		default:
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return result, err
			}

			return ParquetDecimal{Type: ByteArrayDecimal, ByteArrayVal: byteArray}, nil
		}

	case float64:
		// Convert float64 to unscaled int32 or int64 based on precision
		if precision <= 9 {
			unscaledInt := int32(v * math.Pow(10, float64(scale)))
			return ParquetDecimal{Type: Int32Decimal, Int32Val: unscaledInt}, nil
		} else if precision <= 18 {
			unscaledInt := int64(v * math.Pow(10, float64(scale)))
			return ParquetDecimal{Type: Int64Decimal, Int64Val: unscaledInt}, nil
		} else {
			// For higher precision, use *big.Float
			bigFloat := big.NewFloat(v)
			unscaledInt := convertBigFloatToUnscaledInt(bigFloat, scale, scaleFactors)
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return result, err
			}
			return ParquetDecimal{Type: ByteArrayDecimal, ByteArrayVal: byteArray}, nil
		}

	case string:
		// Convert string to unscaled int
		unscaledInt, err := convertDecimalStringToUnscaledInt(v, scale)
		if err != nil {
			return result, err
		}
		if precision <= 9 {
			return ParquetDecimal{Type: Int32Decimal, Int32Val: int32(unscaledInt.Int64())}, nil
		} else if precision <= 18 {
			return ParquetDecimal{Type: Int64Decimal, Int64Val: unscaledInt.Int64()}, nil
		} else {
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return result, err
			}
			return ParquetDecimal{Type: ByteArrayDecimal, ByteArrayVal: byteArray}, nil
		}

	case int64:
		switch {
		case precision <= 9:
			return ParquetDecimal{Type: Int32Decimal, Int32Val: int32(v)}, nil
		case precision <= 18:
			return ParquetDecimal{Type: Int64Decimal, Int64Val: v}, nil
		default:
			unscaledInt := big.NewInt(v)
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return result, err
			}
			return ParquetDecimal{Type: ByteArrayDecimal, ByteArrayVal: byteArray}, nil
		}
	default:
		return result, fmt.Errorf("unsupported type %T for DECIMAL conversion", v)
	}
}

func buildNode(col data.Column) schema.Node {
	mapped := checkType(col)
	if mapped.LogicalType == nil {
		return schema.MustPrimitive(schema.NewPrimitiveNode(col.Name, parquet.Repetitions.Optional, mapped.ParquetType, int32(col.Position), -1))
	}

	var typeLen int
	switch mapped.ParquetType {
	case parquet.Types.FixedLenByteArray:
		if col.Type == "UUID" {
			typeLen = 16
		} else {
			typeLen = calculateBytesForPrecision(int(col.Precision))
		}
	}
	node, err := schema.NewPrimitiveNodeLogical(col.Name, parquet.Repetitions.Optional, mapped.LogicalType, mapped.ParquetType, typeLen, int32(col.Position))
	if err != nil {
		panic(err)
	}
	return node
}

func convertDecimalStringToUnscaledInt(decimalStr string, scale int) (*big.Int, error) {
	if decimalStr == "" {
		return nil, fmt.Errorf("empty decimal string")
	}

	negative := false
	if strings.HasPrefix(decimalStr, "-") {
		negative = true
		decimalStr = decimalStr[1:]
	}

	parts := strings.Split(decimalStr, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid decimal format: %s", decimalStr)
	}

	integerPart := parts[0]
	fractionalPart := ""
	if len(parts) == 2 {
		fractionalPart = parts[1]
	}

	if len(fractionalPart) > scale {
		return nil, fmt.Errorf("fractional part exceeds scale: %s", decimalStr)
	}
	fractionalPart = fractionalPart + strings.Repeat("0", scale-len(fractionalPart))

	unscaledStr := integerPart + fractionalPart
	unscaledValue, success := new(big.Int).SetString(unscaledStr, 10)
	if !success {
		return nil, fmt.Errorf("failed to parse unscaled value: %s", unscaledStr)
	}

	if negative {
		unscaledValue.Neg(unscaledValue)
	}

	return unscaledValue, nil
}

func fastPowerOfTen(scale int, scaleFactors map[int]*big.Float) *big.Float {
	if factor, exists := scaleFactors[scale]; exists {
		return factor
	}

	result := big.NewFloat(1)
	base := big.NewFloat(10)

	for scale > 0 {
		if scale%2 != 0 {
			result.Mul(result, base)
		}
		base.Mul(base, base)
		scale /= 2
	}

	scaleFactors[scale] = result
	return result
}

// ConvertBigFloatToUnscaledInt converts a big.Float to a big.Int by applying the scale
func convertBigFloatToUnscaledInt(value *big.Float, scale int, scaleFactors map[int]*big.Float) *big.Int {
	// Get the scaling factor efficiently
	scalingFactor := fastPowerOfTen(scale, scaleFactors)

	// Multiply the big.Float value by the scaling factor
	scaledValue := new(big.Float).Mul(value, scalingFactor)

	// Convert the scaled value to a big.Int
	unscaledInt := new(big.Int)
	scaledValue.Int(unscaledInt)

	return unscaledInt
}

func calculateBytesForPrecision(precision int) int {
	// Calculate the number of bits needed to represent 10^p - 1
	bitsNeeded := int(math.Ceil(float64(precision)*math.Log2(10))) + 1

	// Calculate the number of bytes needed
	bytesNeeded := int(math.Ceil(float64(bitsNeeded) / 8))

	// Add an extra byte as a safety margin for edge cases with two's complement
	return bytesNeeded
}

func bigIntToFixedBytes(value *big.Int, precision int) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("value cannot be nil")
	}

	size := calculateBytesForPrecision(precision)

	var bytesVal []byte
	if value.Sign() < 0 {
		// Compute two's complement for negative numbers
		absVal := new(big.Int).Abs(value)
		maxValue := new(big.Int).Lsh(big.NewInt(1), uint(8*size)) // 2^(8*size)
		twosComplement := new(big.Int).Sub(maxValue, absVal)
		bytesVal = twosComplement.Bytes()
	} else {
		bytesVal = value.Bytes()
	}

	if len(bytesVal) > size {
		return nil, fmt.Errorf("value %s exceeds %d bytes", value.String(), size)
	}

	fixedBytes := make([]byte, size)
	copy(fixedBytes[size-len(bytesVal):], bytesVal)

	if value.Sign() < 0 {
		// Fill leading bytes with 0xFF for negative numbers
		for i := 0; i < size-len(bytesVal); i++ {
			fixedBytes[i] = 0xFF
		}
	}

	return fixedBytes, nil
}
