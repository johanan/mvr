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

	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/schema"
	"github.com/google/uuid"
	"github.com/johanan/mvr/data"

	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

type ParquetDataWriter struct {
	datastream       *data.DataStream
	writer           *file.Writer
	columnBuffers    []interface{}
	definitionLevels [][]int16
	rowCount         int
	mux              sync.Mutex
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
	"BOOL":        {parquet.Types.Boolean, nil, reflect.TypeOf(bool(false))},
	"INT2":        {parquet.Types.Int32, nil, reflect.TypeOf(int32(0))},
	"INT4":        {parquet.Types.Int32, nil, reflect.TypeOf(int32(0))},
	"INT8":        {parquet.Types.Int64, nil, reflect.TypeOf(int64(0))},
	"FLOAT4":      {parquet.Types.Float, nil, reflect.TypeOf(float32(0))},
	"FLOAT8":      {parquet.Types.Double, nil, reflect.TypeOf(float64(0))},
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
	if col.DatabaseType == "NUMERIC" {
		return numericLookup(int(col.Precision), int(col.Scale))
	}
	if t, ok := parquetTypeMap[col.DatabaseType]; ok {
		return t
	}
	return MappedType{parquet.Types.ByteArray, schema.StringLogicalType{}, reflect.TypeOf(string(""))}
}

func NewParquetDataWriter(datastream *data.DataStream, ioWriter io.Writer) *ParquetDataWriter {
	columnCount := len(datastream.DestColumns)
	maxRows := 100

	columnBuffers := make([]interface{}, columnCount)
	definitionLevels := make([][]int16, columnCount)

	for i := 0; i < columnCount; i++ {
		mapped := checkType(datastream.DestColumns[i])
		columnBuffers[i] = reflect.MakeSlice(reflect.SliceOf(mapped.GoType), maxRows, maxRows).Interface()
		definitionLevels[i] = make([]int16, maxRows)
	}

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

	return &ParquetDataWriter{datastream: datastream, writer: writer, columnBuffers: columnBuffers, definitionLevels: definitionLevels, rowCount: 0}
}

func (pw *ParquetDataWriter) WriteRow(row []any) error {
	pw.mux.Lock()
	defer pw.mux.Unlock()
	for i, col := range pw.datastream.DestColumns {
		if row[i] == nil {
			pw.definitionLevels[i][pw.rowCount] = 0
			continue
		}
		// has value so set definition level to 1
		pw.definitionLevels[i][pw.rowCount] = 1

		switch col.DatabaseType {
		case "INT2":
			buf, ok := pw.columnBuffers[i].([]int32)
			if !ok {
				return fmt.Errorf("type assertion failed for INT2")
			}
			buf[pw.rowCount] = cast.ToInt32(row[i])
		case "INT4":
			buf, ok := pw.columnBuffers[i].([]int32)
			if !ok {
				return fmt.Errorf("type assertion failed for INT4")
			}
			buf[pw.rowCount] = cast.ToInt32(row[i])
		case "INT8":
			buf, ok := pw.columnBuffers[i].([]int64)
			if !ok {
				return fmt.Errorf("type assertion failed for INT8")
			}
			buf[pw.rowCount] = cast.ToInt64(row[i])
		case "FLOAT4":
			buf, ok := pw.columnBuffers[i].([]float32)
			if !ok {
				return fmt.Errorf("type assertion failed for FLOAT4")
			}
			buf[pw.rowCount] = cast.ToFloat32(row[i])
		case "FLOAT8":
			buf, ok := pw.columnBuffers[i].([]float64)
			if !ok {
				return fmt.Errorf("type assertion failed for FLOAT8")
			}
			buf[pw.rowCount] = cast.ToFloat64(row[i])
		case "JSONB", "JSON", "_TEXT":
			buf, ok := pw.columnBuffers[i].([]string)
			if !ok {
				return fmt.Errorf("type assertion failed for JSON types")
			}
			j, err := json.Marshal(row[i])
			if err != nil {
				return fmt.Errorf("failed to marshal JSON for column %s: %v", col.Name, err)
			}
			buf[pw.rowCount] = string(j)
		case "UUID":
			buf, ok := pw.columnBuffers[i].([][]byte)
			if !ok {
				return fmt.Errorf("type assertion failed for UUID")
			}

			switch v := row[i].(type) {
			case string:
				uuidValue, err := uuid.Parse(v)
				if err != nil {
					return fmt.Errorf("failed to parse UUID for column %s: %v", col.Name, err)
				}
				buf[pw.rowCount] = uuidValue[:]
			case [16]uint8:
				buf[pw.rowCount] = v[:]
			case uuid.UUID:
				buf[pw.rowCount] = v[:]
			default:
				return fmt.Errorf("expected string or UUID for UUID column %s, got %T", col.Name, v)
			}
		case "NUMERIC":
			decimalValue, err := convertToParquetDecimal(row[i], int(col.Precision), int(col.Scale))
			if err != nil {
				return fmt.Errorf("failed to convert DECIMAL for column %s: %v", col.Name, err)
			}
			switch decimalValue.Type {
			case Int32Decimal:
				buf, ok := pw.columnBuffers[i].([]int32)
				if !ok {
					return fmt.Errorf("type assertion failed for DECIMAL")
				}
				buf[pw.rowCount] = decimalValue.Int32Val
			case Int64Decimal:
				buf, ok := pw.columnBuffers[i].([]int64)
				if !ok {
					return fmt.Errorf("type assertion failed for DECIMAL")
				}
				buf[pw.rowCount] = decimalValue.Int64Val
			case ByteArrayDecimal:
				buf, ok := pw.columnBuffers[i].([][]byte)
				if !ok {
					return fmt.Errorf("type assertion failed for DECIMAL")
				}
				buf[pw.rowCount] = decimalValue.ByteArrayVal
			default:
				return fmt.Errorf("unknown DECIMAL type %v", decimalValue.Type)
			}
		case "DATE":
			buf, ok := pw.columnBuffers[i].([]int32)
			if !ok {
				return fmt.Errorf("type assertion failed for DATE")
			}
			dateValue, ok := row[i].(time.Time)
			if !ok {
				return fmt.Errorf("expected time.Time for DATE column %s, got %T", col.Name, row[i])
			}
			// Convert the date to the number of days since the Unix epoch
			daysSinceEpoch := int32(dateValue.Sub(epochDate).Truncate(24*time.Hour).Hours() / 24)
			buf[pw.rowCount] = daysSinceEpoch
		case "BOOL":
			buf, ok := pw.columnBuffers[i].([]bool)
			if !ok {
				return fmt.Errorf("type assertion failed for BOOL")
			}
			switch v := row[i].(type) {
			case bool:
				buf[pw.rowCount] = v
			default:
				return fmt.Errorf("expected bool for BOOL column %s, got %T", col.Name, row[i])
			}
		case "TIMESTAMP", "TIMESTAMPTZ":
			buf, ok := pw.columnBuffers[i].([]int64)
			if !ok {
				return fmt.Errorf("type assertion failed for TIMESTAMP")
			}
			v, ok := row[i].(time.Time)
			if !ok {
				return fmt.Errorf("expected time.Time for TIMESTAMP column %s, got %T", col.Name, row[i])
			}
			buf[pw.rowCount] = v.UnixNano()
		default:
			// cast everything else to string
			buf, ok := pw.columnBuffers[i].([]string)
			if !ok {
				return fmt.Errorf("type assertion failed for default")
			}
			buf[pw.rowCount] = cast.ToString(row[i])
		}
	}

	pw.rowCount++

	if pw.rowCount >= 100 {
		err := pw.writeBatch()
		if err != nil {
			return err
		}
		// Reset the row count and clear the buffers
		pw.rowCount = 0
		for i := range pw.datastream.DestColumns {
			mapped := checkType(pw.datastream.DestColumns[i])
			pw.columnBuffers[i] = reflect.MakeSlice(reflect.SliceOf(mapped.GoType), 100, 100).Interface()
			pw.definitionLevels[i] = make([]int16, 100)
		}
	}

	return nil
}

func (pw *ParquetDataWriter) Flush() error {
	if pw.rowCount > 0 {
		pw.mux.Lock()
		err := pw.writeBatch()
		if err != nil {
			return err
		}
		pw.rowCount = 0
		for i := range pw.datastream.DestColumns {
			mapped := checkType(pw.datastream.DestColumns[i])
			pw.columnBuffers[i] = reflect.MakeSlice(reflect.SliceOf(mapped.GoType), 100, 100).Interface()
			pw.definitionLevels[i] = make([]int16, 100)
		}
	}
	return nil
}

func (pw *ParquetDataWriter) Close() error {
	return pw.writer.Close()
}

func (pw *ParquetDataWriter) writeBatch() error {
	rg := pw.writer.AppendBufferedRowGroup()
	defer rg.Close()
	for i := range pw.datastream.DestColumns {
		rgCol, err := rg.Column(i)
		if err != nil {
			return err
		}

		switch chunker := rgCol.(type) {
		case *file.Int32ColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([]int32)
			if !ok {
				return fmt.Errorf("type assertion failed for INT4")
			}
			_, err := chunker.WriteBatch(buf[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		case *file.Int64ColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([]int64)
			if !ok {
				return fmt.Errorf("type assertion failed for INT8 writer")
			}
			_, err := chunker.WriteBatch(buf[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		case *file.Float32ColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([]float32)
			if !ok {
				return fmt.Errorf("type assertion failed for FLOAT4")
			}
			_, err := chunker.WriteBatch(buf[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		case *file.Float64ColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([]float64)
			if !ok {
				return fmt.Errorf("type assertion failed for FLOAT8")
			}
			_, err := chunker.WriteBatch(buf[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		case *file.ByteArrayColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([]string)
			if !ok {
				return fmt.Errorf("type assertion failed for BYTE_ARRAY")
			}
			byteArray := make([]parquet.ByteArray, pw.rowCount)
			for j := 0; j < pw.rowCount; j++ {
				byteArray[j] = parquet.ByteArray(buf[j])
			}
			_, err := chunker.WriteBatch(byteArray[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		case *file.FixedLenByteArrayColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([][]byte)
			if !ok {
				return fmt.Errorf("type assertion failed for FIXED_LEN_BYTE_ARRAY")
			}
			// loop over and set as parquet.FixedLenByteArray
			fixedLenByteArray := make([]parquet.FixedLenByteArray, pw.rowCount)
			for j := 0; j < pw.rowCount; j++ {
				fixedLenByteArray[j] = parquet.FixedLenByteArray(buf[j])
			}
			_, err := chunker.WriteBatch(fixedLenByteArray[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		case *file.BooleanColumnChunkWriter:
			buf, ok := pw.columnBuffers[i].([]bool)
			if !ok {
				return fmt.Errorf("type assertion failed for BOOLEAN")
			}
			_, err := chunker.WriteBatch(buf[:pw.rowCount], pw.definitionLevels[i][:pw.rowCount], nil)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("type assertion failed for default")
		}
	}
	return nil
}

func convertToParquetDecimal(value any, precision, scale int) (ParquetDecimal, error) {
	var result ParquetDecimal
	switch v := value.(type) {
	case decimal.Decimal:
		// I tried big float but it was not working
		float := v.StringFixed(int32(scale))
		return convertToParquetDecimal(float, precision, scale)
	case *big.Float:
		// Convert *big.Float to unscaled int
		unscaledInt := convertBigFloatToUnscaledInt(v, scale)
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
			unscaledInt := convertBigFloatToUnscaledInt(bigFloat, scale)
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
			return result, fmt.Errorf("unsupported precision %d for int64 DECIMAL conversion", precision)
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
		if col.DatabaseType == "UUID" {
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

var scaleFactors = make(map[int]*big.Float)

func fastPowerOfTen(scale int) *big.Float {
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
func convertBigFloatToUnscaledInt(value *big.Float, scale int) *big.Int {
	// Get the scaling factor efficiently
	scalingFactor := fastPowerOfTen(scale)

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
