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

	"github.com/google/uuid"
	"github.com/johanan/mvr/data"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	"github.com/shopspring/decimal"
	"github.com/spf13/cast"
)

var epochDate = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

type ParquetDataWriter struct {
	datastream *data.DataStream
	writer     *parquet.GenericWriter[map[string]any]
	buffer     []map[string]any
	mux        sync.Mutex
}

func NewParquetDataWriter(datastream *data.DataStream, ioWriter io.Writer) *ParquetDataWriter {
	config, err := parquet.NewWriterConfig()
	if err != nil {
		panic(err)
	}
	r := &RootNode{Columns: make([]ColumnField, len(datastream.DestColumns))}
	for i, col := range datastream.DestColumns {
		r.Columns[i] = ColumnField{Column: col, Node: nodeOf(col)}
	}
	schema := parquet.NewSchema("mvr", r)

	config.Schema = schema
	config.Compression = &parquet.Snappy
	config.DataPageStatistics = true

	return &ParquetDataWriter{datastream: datastream, writer: parquet.NewGenericWriter[map[string]any](ioWriter, config), buffer: make([]map[string]any, 0, 100)}
}

func (pw *ParquetDataWriter) WriteRow(row []any) error {
	// Create a map of column names to values
	rowMap := make(map[string]any, len(pw.datastream.DestColumns))
	for i, col := range pw.datastream.DestColumns {
		if row[i] == nil {
			rowMap[col.Name] = nil
			continue
		}

		switch col.DatabaseType {
		case "INT2":
			rowMap[col.Name] = cast.ToInt16(row[i])
		case "INT4":
			rowMap[col.Name] = cast.ToInt32(row[i])
		case "INT8":
			rowMap[col.Name] = cast.ToInt64(row[i])
		case "JSONB", "JSON", "_TEXT":
			j, err := json.Marshal(row[i])
			if err != nil {
				return fmt.Errorf("failed to marshal JSON for column %s: %v", col.Name, err)
			}
			rowMap[col.Name] = string(j)
		case "UUID":
			switch v := row[i].(type) {
			case string:
				uuidValue, err := uuid.Parse(v)
				if err != nil {
					return fmt.Errorf("failed to parse UUID for column %s: %v", col.Name, err)
				}
				rowMap[col.Name] = uuidValue
			case [16]uint8:
				rowMap[col.Name] = v
			case uuid.UUID:
				rowMap[col.Name] = v
			default:
				return fmt.Errorf("expected string or UUID for UUID column %s, got %T", col.Name, v)
			}
		case "NUMERIC", "DECIMAL":
			decimalValue, err := convertToParquetDecimal(row[i], int(col.Precision), int(col.Scale))
			if err != nil {
				return fmt.Errorf("failed to convert DECIMAL for column %s: %v", col.Name, err)
			}
			rowMap[col.Name] = decimalValue
		case "DATE":
			dateValue, ok := row[i].(time.Time)
			if !ok {
				return fmt.Errorf("expected time.Time for DATE column %s, got %T", col.Name, row[i])
			}
			// Convert the date to the number of days since the Unix epoch
			daysSinceEpoch := int32(dateValue.Sub(epochDate).Truncate(24*time.Hour).Hours() / 24)
			rowMap[col.Name] = daysSinceEpoch
		case "BOOL":
			switch v := row[i].(type) {
			case bool:
				rowMap[col.Name] = v
			default:
				return fmt.Errorf("expected bool for BOOL column %s, got %T", col.Name, row[i])
			}
		default:
			rowMap[col.Name] = row[i]
		}
	}

	pw.buffer = append(pw.buffer, rowMap)

	if len(pw.buffer) >= 100 {
		pw.mux.Lock()
		_, err := pw.writer.Write(pw.buffer)
		if err != nil {
			return err
		}
		pw.buffer = pw.buffer[:0]
		pw.mux.Unlock()
	}

	return nil
}

func convertToParquetDecimal(value any, precision, scale int) (any, error) {
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
				return nil, fmt.Errorf("failed to convert DECIMAL to int32: %v", unscaledInt)
			}
			return int32(unscaledInt.Int64()), nil

		case precision <= 18:
			if !unscaledInt.IsInt64() {
				return nil, fmt.Errorf("failed to convert DECIMAL to int64: %v", unscaledInt)
			}
			return unscaledInt.Int64(), nil

		default:
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return nil, err
			}

			return byteArray, nil
		}

	case float64:
		// Convert float64 to unscaled int32 or int64 based on precision
		if precision <= 9 {
			unscaledInt := int32(v * math.Pow(10, float64(scale)))
			return unscaledInt, nil
		} else if precision <= 18 {
			unscaledInt := int64(v * math.Pow(10, float64(scale)))
			return unscaledInt, nil
		} else {
			// For higher precision, use *big.Float
			bigFloat := big.NewFloat(v)
			unscaledInt := convertBigFloatToUnscaledInt(bigFloat, scale)
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return nil, err
			}
			return byteArray, nil
		}

	case string:
		// Convert string to unscaled int
		unscaledInt, err := convertDecimalStringToUnscaledInt(v, scale)
		if err != nil {
			return nil, err
		}
		if precision <= 9 {
			return int32(unscaledInt.Int64()), nil
		} else if precision <= 18 {
			return unscaledInt.Int64(), nil
		} else {
			byteArray, err := bigIntToFixedBytes(unscaledInt, precision)
			if err != nil {
				return nil, err
			}
			return byteArray, nil
		}

	case int64:
		switch {
		case precision <= 9:
			return int32(v), nil
		case precision <= 18:
			return v, nil
		default:
			return nil, fmt.Errorf("unsupported precision %d for int64 DECIMAL conversion", precision)
		}
	default:
		return nil, fmt.Errorf("unsupported type %T for DECIMAL conversion", v)
	}
}

func (pw *ParquetDataWriter) Flush() error {
	if len(pw.buffer) > 0 {
		pw.mux.Lock()
		_, err := pw.writer.Write(pw.buffer)
		if err != nil {
			return err
		}
		pw.buffer = pw.buffer[:0]
		pw.mux.Unlock()
	}
	return pw.writer.Flush()
}

func (pw *ParquetDataWriter) Close() error {
	return pw.writer.Close()
}

type ColumnField struct {
	parquet.Node
	Column data.Column
}

func (c *ColumnField) Name() string { return c.Column.Name }
func (c *ColumnField) Value(base reflect.Value) reflect.Value {
	switch base.Kind() {
	case reflect.Map:
		return base.MapIndex(reflect.ValueOf(&c.Column.Name).Elem())
	case reflect.Ptr:
		if base.IsNil() {
			base.Set(reflect.New(base.Type().Elem()))
		}
		return base.FieldByIndex([]int{c.Column.Position})
	default:
		return base.FieldByIndex([]int{c.Column.Position})
	}
}

type RootNode struct {
	Columns []ColumnField
}

func (r *RootNode) ID() int            { return 0 }
func (r *RootNode) String() string     { return "" }
func (r *RootNode) Type() parquet.Type { return groupType{} }
func (r *RootNode) Optional() bool     { return false }
func (r *RootNode) Repeated() bool     { return false }
func (r *RootNode) Required() bool     { return true }
func (r *RootNode) Leaf() bool         { return false }
func (r *RootNode) Fields() []parquet.Field {
	fields := make([]parquet.Field, len(r.Columns))
	for i, col := range r.Columns {
		fields[i] = &ColumnField{Column: col.Column, Node: col.Node}
	}
	return fields
}
func (r *RootNode) Encoding() encoding.Encoding { return nil }
func (r *RootNode) Compression() compress.Codec { return nil }
func (r *RootNode) GoType() reflect.Type        { return nil }

func nodeOf(col data.Column) parquet.Node {
	switch col.DatabaseType {
	case "BOOL":
		return optional(parquet.Leaf(parquet.BooleanType), col)
	case "INT2":
		return optional(parquet.Int(16), col)
	case "INT4":
		return optional(parquet.Int(32), col)
	case "INT8":
		return optional(parquet.Int(64), col)
	case "UUID":
		return optional(parquet.UUID(), col)
	case "FLOAT8", "FLOAT4":
		// for some reason both scan in as float64
		return optional(parquet.Leaf(parquet.DoubleType), col)
	case "NUMERIC", "DECIMAL":
		// Determine the appropriate type based on precision
		if col.Precision <= 9 {
			return optional(parquet.Decimal(int(col.Scale), int(col.Precision), parquet.Int32Type), col)
		} else if col.Precision <= 18 {
			return optional(parquet.Decimal(int(col.Scale), int(col.Precision), parquet.Int64Type), col)
		} else {
			// 16 bytes for 128-bit decimal. That will cover DECIMAL(38, X) which should give us
			// coverage for Snowflake
			arrayLen := calculateBytesForPrecision(int(col.Precision))
			fixed := parquet.FixedLenByteArrayType(arrayLen)
			return optional(parquet.Decimal(int(col.Scale), int(col.Precision), fixed), col)
		}
	case "DATE":
		return optional(parquet.Date(), col)
	case "TIMESTAMP":
		return optional(TimestampNTZ(parquet.Nanosecond), col)
	case "TIMESTAMPTZ":
		return optional(parquet.Timestamp(parquet.Nanosecond), col)
	case "VARCHAR":
		return optional(parquet.String(), col)
	case "JSONB":
		return optional(parquet.JSON(), col)
	default:
		return optional(parquet.String(), col)
	}
}

func optional(node parquet.Node, col data.Column) parquet.Node {
	if col.Nullable {
		return parquet.Optional(node)
	}
	return parquet.Optional(node)
}

// pulled from library to get a type to use
type groupType struct{}

func (groupType) String() string { return "group" }
func (groupType) Kind() parquet.Kind {
	panic("cannot call Kind on parquet group")
}
func (groupType) Compare(parquet.Value, parquet.Value) int {
	panic("cannot compare values on parquet group")
}
func (groupType) NewColumnIndexer(int) parquet.ColumnIndexer {
	panic("cannot create column indexer from parquet group")
}
func (groupType) NewDictionary(int, int, encoding.Values) parquet.Dictionary {
	panic("cannot create dictionary from parquet group")
}
func (t groupType) NewColumnBuffer(int, int) parquet.ColumnBuffer {
	panic("cannot create column buffer from parquet group")
}
func (t groupType) NewPage(int, int, encoding.Values) parquet.Page {
	panic("cannot create page from parquet group")
}
func (t groupType) NewValues(_ []byte, _ []uint32) encoding.Values {
	panic("cannot create values from parquet group")
}
func (groupType) Encode(_ []byte, _ encoding.Values, _ encoding.Encoding) ([]byte, error) {
	panic("cannot encode parquet group")
}
func (groupType) Decode(_ encoding.Values, _ []byte, _ encoding.Encoding) (encoding.Values, error) {
	panic("cannot decode parquet group")
}
func (groupType) EstimateDecodeSize(_ int, _ []byte, _ encoding.Encoding) int {
	panic("cannot estimate decode size of parquet group")
}
func (groupType) AssignValue(reflect.Value, parquet.Value) error {
	panic("cannot assign value to a parquet group")
}
func (t groupType) ConvertValue(parquet.Value, parquet.Type) (parquet.Value, error) {
	panic("cannot convert value to a parquet group")
}
func (groupType) Length() int                              { return 0 }
func (groupType) EstimateSize(int) int                     { return 0 }
func (groupType) EstimateNumValues(int) int                { return 0 }
func (groupType) ColumnOrder() *format.ColumnOrder         { return nil }
func (groupType) PhysicalType() *format.Type               { return nil }
func (groupType) LogicalType() *format.LogicalType         { return nil }
func (groupType) ConvertedType() *deprecated.ConvertedType { return nil }

// copied from library so that we can use it here
type timestampType struct {
	IsAdjustedToUTC bool
	Unit            format.TimeUnit
	baseType        parquet.Type
}

func TimestampNTZ(unit parquet.TimeUnit) parquet.Node {
	// Create a node for int64 and extract its type
	int64Node := parquet.Int(64)
	baseType := int64Node.Type()

	return parquet.Leaf(&timestampType{
		IsAdjustedToUTC: false,
		Unit:            unit.TimeUnit(),
		baseType:        baseType,
	})
}

func (t *timestampType) String() string {
	return (&format.TimestampType{
		IsAdjustedToUTC: t.IsAdjustedToUTC,
		Unit:            t.Unit,
	}).String()
}

func (t *timestampType) Kind() parquet.Kind {
	return t.baseType.Kind()
}

func (t *timestampType) Length() int {
	return t.baseType.Length()
}

func (t *timestampType) EstimateSize(n int) int { return t.baseType.EstimateSize(n) }

func (t *timestampType) EstimateNumValues(n int) int { return t.baseType.EstimateNumValues(n) }

func (t *timestampType) Compare(a, b parquet.Value) int {
	return t.baseType.Compare(a, b)
}

func (t *timestampType) ColumnOrder() *format.ColumnOrder {
	return t.baseType.ColumnOrder()
}

func (t *timestampType) PhysicalType() *format.Type {
	return t.baseType.PhysicalType()
}

func (t *timestampType) LogicalType() *format.LogicalType {
	return &format.LogicalType{
		Timestamp: &format.TimestampType{
			IsAdjustedToUTC: t.IsAdjustedToUTC,
			Unit:            t.Unit,
		},
	}
}

func (t *timestampType) ConvertedType() *deprecated.ConvertedType {
	switch {
	case t.Unit.Millis != nil:
		ct := deprecated.TimestampMillis
		return &ct
	case t.Unit.Micros != nil:
		ct := deprecated.TimestampMicros
		return &ct
	default:
		return nil
	}
}

func (t *timestampType) NewColumnIndexer(sizeLimit int) parquet.ColumnIndexer {
	return t.baseType.NewColumnIndexer(sizeLimit)
}

func (t *timestampType) NewColumnBuffer(columnIndex int, bufferSize int) parquet.ColumnBuffer {
	return t.baseType.NewColumnBuffer(columnIndex, bufferSize)
}

func (t *timestampType) NewDictionary(columnIndex, numValues int, data encoding.Values) parquet.Dictionary {
	return t.baseType.NewDictionary(columnIndex, numValues, data)
}

func (t *timestampType) NewPage(columnIndex, numValues int, data encoding.Values) parquet.Page {
	return t.baseType.NewPage(columnIndex, numValues, data)
}

func (t *timestampType) NewValues(values []byte, offsets []uint32) encoding.Values {
	return t.baseType.NewValues(values, offsets)
}

func (t *timestampType) Encode(dst []byte, src encoding.Values, enc encoding.Encoding) ([]byte, error) {
	return t.baseType.Encode(dst, src, enc)
}

func (t *timestampType) Decode(dst encoding.Values, src []byte, enc encoding.Encoding) (encoding.Values, error) {
	return t.baseType.Decode(dst, src, enc)
}

func (t *timestampType) EstimateDecodeSize(numValues int, src []byte, enc encoding.Encoding) int {
	return t.baseType.EstimateDecodeSize(numValues, src, enc)
}

func (t *timestampType) AssignValue(dst reflect.Value, src parquet.Value) error {
	return t.baseType.AssignValue(dst, src)
}

func (t *timestampType) ConvertValue(val parquet.Value, typ parquet.Type) (parquet.Value, error) {
	// breaking this to just get going
	return val, nil
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
