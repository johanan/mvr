package file

import (
	"io"
	"reflect"
	"sync"

	"github.com/johanan/mvr/data"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	"github.com/spf13/cast"
)

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
	r := &RootNode{Columns: make([]ColumnField, len(datastream.Columns))}
	for i, col := range datastream.Columns {
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
	rowMap := make(map[string]any, len(pw.datastream.Columns))
	for i, col := range pw.datastream.Columns {
		switch col.DatabaseType {
		case "INT2":
			rowMap[col.Name] = cast.ToInt16(row[i])
		case "INT4":
			rowMap[col.Name] = cast.ToInt32(row[i])
		case "INT8":
			rowMap[col.Name] = cast.ToInt64(row[i])
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
	case "INT2":
		return optional(parquet.Int(16), col)
	case "INT4":
		return optional(parquet.Int(32), col)
	case "INT8":
		return optional(parquet.Int(64), col)
	case "UUID":
		return optional(parquet.UUID(), col)
	case "TIMESTAMP":
		return optional(TimestampNTZ(parquet.Nanosecond), col)
	case "TIMESTAMPTZ":
		return optional(parquet.Timestamp(parquet.Nanosecond), col)
	case "VARCHAR":
		return optional(parquet.String(), col)
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
