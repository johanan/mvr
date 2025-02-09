package file

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/big"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/shopspring/decimal"
	"github.com/zeebo/assert"
)

func TestDecimalToFixedBytes(t *testing.T) {
	tests := []struct {
		name        string
		decimalStr  string
		scale       int
		precision   int
		expected    string // Hex representation
		expectError bool
	}{
		{
			name:        "Positive Standard",
			decimalStr:  "639529.823302734000000",
			scale:       15,
			precision:   38,
			expected:    "0000000000000022ab4259eb6bcb2f80",
			expectError: false,
		},
	}

	for _, tt := range tests {
		tc := tt // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			value := decimal.RequireFromString(tc.decimalStr)

			decimalStr := value.StringFixed(int32(tc.scale))
			assert.Equal(t, tc.decimalStr, decimalStr)

			result, err := convertDecimalStringToUnscaledInt(decimalStr, tt.scale)
			assert.NoError(t, err)
			converted, err := bigIntToFixedBytes(result, tc.precision)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, hex.EncodeToString(converted))
			}
		})
	}
}

func newBigIntFromString(s string) *big.Int {
	i := new(big.Int)
	i.SetString(s, 10)
	return i
}

func TestConversionToFixedBytes(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		scale     int
		precision int
		expected  string // Hex representation
	}{
		{
			name:      "Positive Standard",
			value:     "639529.823302734000000",
			scale:     15,
			precision: 38,
			expected:  "0000000000000022ab4259eb6bcb2f80",
		},
		{
			name: "Positive Standard",
			// sometimes the value for a decimal is already an int
			value:     newBigIntFromString("639529"),
			scale:     15,
			precision: 38,
			expected:  "0000000000000000000000000009c229",
		},
	}
	scaleFactor := make(map[int]*big.Float)
	for _, tt := range tests { // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToParquetDecimal(tt.value, tt.precision, tt.scale, scaleFactor)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, hex.EncodeToString(result.ByteArrayVal))
		})
	}
}

func TestBigIntToFixedBytes(t *testing.T) {
	tests := []struct {
		name        string
		decimalStr  string
		scale       int
		precision   int
		expected    string // Hex representation
		expectError bool
	}{
		{
			name:        "Positive Standard",
			decimalStr:  "123456789012345.123456789012345",
			scale:       15,
			precision:   38,
			expected:    "000000018ee90ff6c371e7c1d1605f79",
			expectError: false,
		},
		{
			name:        "Negative Standard",
			decimalStr:  "-123456789012345.123456789012345",
			scale:       15,
			precision:   38,
			expected:    "fffffffe7116f0093c8e183e2e9fa087",
			expectError: false,
		},
		{
			name:        "Small Negative Number",
			decimalStr:  "-25.34",
			scale:       2,
			precision:   8,
			expected:    "fffff61a",
			expectError: false,
		},
		{
			name:        "Zero Value",
			decimalStr:  "0",
			scale:       2,
			precision:   38,
			expected:    "00000000000000000000000000000000",
			expectError: false,
		},
		{
			name:        "Large Positive Number",
			decimalStr:  "12345678901234567890123456789012345678",
			scale:       0,
			precision:   38,
			expected:    "0949b0f6f0023313c4499050de38f34e",
			expectError: false,
		},
		{
			name:        "Fractional Part Exactly Scale",
			decimalStr:  "123.456",
			scale:       3,
			precision:   8,
			expected:    "0001e240", // 123456 in hex is 0x1e240
			expectError: false,
		},
	}

	for _, tt := range tests {
		tc := tt // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			unscaledInt, err := convertDecimalStringToUnscaledInt(tc.decimalStr, tc.scale)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			fixedBytes, err := bigIntToFixedBytes(unscaledInt, tc.precision)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, hex.EncodeToString(fixedBytes))
			}
		})
	}
}

func TestConvertDecimalStringToUnscaledInt(t *testing.T) {
	tests := []struct {
		name           string
		decimalStr     string
		scale          int
		expectedResult string
		expectError    bool
	}{
		{
			name:           "Standard Decimal",
			decimalStr:     "123.45",
			scale:          2,
			expectedResult: "12345",
			expectError:    false,
		},
		{
			name:           "No Decimal Point",
			decimalStr:     "123",
			scale:          2,
			expectedResult: "12300",
			expectError:    false,
		},
		{
			name:           "Fractional Part Less Than Scale",
			decimalStr:     "123.4",
			scale:          2,
			expectedResult: "12340",
			expectError:    false,
		},
		{
			name:           "Fractional Part Equals Scale",
			decimalStr:     "123.45",
			scale:          2,
			expectedResult: "12345",
			expectError:    false,
		},
		{
			name:           "Fractional Part Exceeds Scale",
			decimalStr:     "123.456",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Negative Decimal",
			decimalStr:     "-123.45",
			scale:          2,
			expectedResult: "-12345",
			expectError:    false,
		},
		{
			name:           "Zero Value",
			decimalStr:     "0",
			scale:          2,
			expectedResult: "0",
			expectError:    false,
		},
		{
			name:           "Large Number Without Decimal",
			decimalStr:     "12345678901234567890",
			scale:          5,
			expectedResult: "1234567890123456789000000",
			expectError:    false,
		},
		{
			name:           "Large Number With Decimal",
			decimalStr:     "639529.823302734000000",
			scale:          15,
			expectedResult: "639529823302734000000",
			expectError:    false,
		},
		{
			name:           "Leading Zeros",
			decimalStr:     "000123.45",
			scale:          2,
			expectedResult: "12345",
			expectError:    false,
		},
		{
			name:           "Only Fractional Part",
			decimalStr:     ".45",
			scale:          2,
			expectedResult: "45",
			expectError:    false,
		},
		{
			name:           "Empty String",
			decimalStr:     "",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Invalid Format Multiple Decimals",
			decimalStr:     "123.45.67",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Non-Numeric Characters",
			decimalStr:     "123.4a",
			scale:          2,
			expectedResult: "",
			expectError:    true,
		},
		{
			name:           "Negative Number Without Decimal",
			decimalStr:     "-123",
			scale:          3,
			expectedResult: "-123000",
			expectError:    false,
		},
		{
			name:           "Fractional Part Exactly Scale",
			decimalStr:     "123.456",
			scale:          3,
			expectedResult: "123456",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertDecimalStringToUnscaledInt(tt.decimalStr, tt.scale)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result.String())
			}
		})
	}
}

type parquetTypes interface {
	~int32 | ~int64 | ~float32 | ~float64 | ~string | ~bool | ~[]byte
}

func GetRowGroupColumn[T parquetTypes](reader *file.Reader, colIndex int) ([]T, []int16, error) {
	// for now we just have one row group
	rgr := reader.RowGroup(0)
	rgMeta := rgr.MetaData()
	numRows := rgMeta.NumRows()

	column, _ := rgr.Column(colIndex)
	chunkMeta, _ := rgMeta.ColumnChunk(colIndex)
	stats, _ := chunkMeta.Statistics()
	numValues := stats.NumValues()
	// numValues := chunkMeta.NumValues()

	defLevels := make([]int16, numRows)
	values := make([]T, numValues)
	switch any(*new(T)).(type) {
	case int32:
		r, _ := column.(*file.Int32ColumnChunkReader)
		int32Values := any(values).([]int32)
		r.ReadBatch(numRows, int32Values, defLevels, nil)
	case int64:
		r, _ := column.(*file.Int64ColumnChunkReader)
		int64Values := any(values).([]int64)
		r.ReadBatch(numRows, int64Values, defLevels, nil)
	case float32:
		r, _ := column.(*file.Float32ColumnChunkReader)
		float32Values := any(values).([]float32)
		r.ReadBatch(numRows, float32Values, defLevels, nil)
	case float64:
		r, _ := column.(*file.Float64ColumnChunkReader)
		float64Values := any(values).([]float64)
		r.ReadBatch(numRows, float64Values, defLevels, nil)
	case string:
		r, _ := column.(*file.ByteArrayColumnChunkReader)
		byteArrayValues := make([]parquet.ByteArray, numValues)
		r.ReadBatch(numRows, byteArrayValues, defLevels, nil)
		// Convert parquet.ByteArray to string
		values = make([]T, numValues)
		stringValues := any(values).([]string)
		for i, ba := range byteArrayValues {
			stringValues[i] = string(ba)
		}
	case []byte:
		r, _ := column.(*file.FixedLenByteArrayColumnChunkReader)
		byteArrayValues := make([]parquet.FixedLenByteArray, numValues)
		r.ReadBatch(numRows, byteArrayValues, defLevels, nil)
		// Convert parquet.FixedLenByteArray to []byte
		values = make([]T, numValues)
		byteValues := any(values).([][]byte)
		for i, ba := range byteArrayValues {
			byteValues[i] = ba
		}
	}
	return values, defLevels, nil
}

func testParquetColumn[T parquetTypes](t *testing.T, reader *file.Reader, colIndex int, expected []T, defExpected []int16) {
	values, defLevels, err := GetRowGroupColumn[T](reader, colIndex)
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
	assert.Equal(t, defExpected, defLevels)
}

func TestFullRoundTripToPG(t *testing.T) {
	tests := []struct {
		name        string
		config      *data.StreamConfig
		expected    interface{}
		defExpected []int16
	}{
		{
			name:        "INT 2",
			config:      &data.StreamConfig{SQL: "SELECT smallint_value FROM public.numbers", Format: "parquet"},
			expected:    []int32{int32(1), int32(2), int32(3), int32(32767), int32(0)},
			defExpected: []int16{1, 1, 1, 1, 1},
		},
		{
			name:        "Strings Test",
			config:      &data.StreamConfig{SQL: "SELECT varchar_value FROM public.strings", Format: "parquet"},
			expected:    []string{"a", "b"},
			defExpected: []int16{1, 1},
		},
		{
			name:        "Users Name Test",
			config:      &data.StreamConfig{SQL: "SELECT name FROM public.users", Format: "parquet"},
			expected:    []string{"John Doe", "Test Tester"},
			defExpected: []int16{1, 1},
		},
		{
			name:        "Users Created Test",
			config:      &data.StreamConfig{SQL: "SELECT created FROM public.users", Format: "parquet"},
			expected:    []int64{1728408120000000000, 1728408120000000000},
			defExpected: []int16{1, 1},
		},
		{
			name:        "Users CreatedZ Test",
			config:      &data.StreamConfig{SQL: "SELECT createdz FROM public.users", Format: "parquet"},
			expected:    []int64{1728408120000000000, 1728408120000000000},
			defExpected: []int16{1, 1},
		},
		{
			name:   "Users Unique ID Test",
			config: &data.StreamConfig{SQL: "SELECT unique_id FROM public.users", Format: "parquet"},
			expected: [][]byte{
				{160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17},
				{160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 18},
			},
			defExpected: []int16{1, 1},
		},
		{
			name:        "Users Nullable ID Test",
			config:      &data.StreamConfig{SQL: "SELECT nullable_id FROM public.users", Format: "parquet"},
			expected:    [][]byte{},
			defExpected: []int16{0, 0},
		},
		{
			name:        "Users Active Test",
			config:      &data.StreamConfig{SQL: "SELECT active FROM public.users", Format: "parquet"},
			expected:    []bool{true, false},
			defExpected: []int16{1, 1},
		},
		{
			name:        "Users UUID to string Test",
			config:      &data.StreamConfig{SQL: "SELECT unique_id FROM public.users", Format: "parquet", Columns: []data.Column{{Name: "unique_id", Type: "TEXT"}}},
			expected:    []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"},
			defExpected: []int16{1, 1},
		},
		{
			name:        "Decimal to Double Test",
			config:      &data.StreamConfig{SQL: "SELECT decimal_value FROM public.numbers LIMIT 1", Format: "parquet", Columns: []data.Column{{Name: "decimal_value", Type: "DOUBLE"}}},
			expected:    []float64{1},
			defExpected: []int16{1},
		},
		{
			name:        "Decimal to Int Test",
			config:      &data.StreamConfig{SQL: "SELECT decimal_value FROM public.numbers LIMIT 1", Format: "parquet", Columns: []data.Column{{Name: "decimal_value", Type: "INT8"}}},
			expected:    []int64{1},
			defExpected: []int16{1},
		},
	}
	os.Setenv("TZ", "UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer
			writeCloser := NewWriteCloseBuffer(&buf)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			local_url, _ := url.Parse(local_db_url)

			pgr, _ := database.NewPGDataReader(local_url)
			pgDs, _ := pgr.CreateDataStream(ctx, local_url, tt.config)
			defer pgr.Close()

			writer := NewParquetDataWriter(pgDs, writeCloser)

			err := core.Execute(ctx, 1, tt.config, pgDs, pgr, writer)
			assert.NoError(t, err)
			writer.Close()

			reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(nil)))
			assert.NoError(t, err)
			switch expected := tt.expected.(type) {
			case []int32:
				testParquetColumn(t, reader, 0, expected, tt.defExpected)
			case []int64:
				testParquetColumn(t, reader, 0, expected, tt.defExpected)
			case []float32:
				testParquetColumn(t, reader, 0, expected, tt.defExpected)
			case []float64:
				testParquetColumn(t, reader, 0, expected, tt.defExpected)
			case []string:
				testParquetColumn(t, reader, 0, expected, tt.defExpected)
			case [][]byte:
				testParquetColumn(t, reader, 0, expected, tt.defExpected)
			}
		})
	}
	os.Unsetenv("TZ")
}

func TestParquetWriter(t *testing.T) {
	var buf bytes.Buffer
	writeCloser := NewWriteCloseBuffer(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchChan := make(chan data.Batch, 10)
	ds := &data.DataStream{
		BatchChan: batchChan,
		BatchSize: 10,
		Mux:       sync.Mutex{},
		DestColumns: []data.Column{
			{Name: "id", Type: "BIGINT"},
			{Name: "name", Type: "TEXT"},
		},
	}

	pdw := NewParquetDataWriter(ds, writeCloser)

	rows := [][]any{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), nil},
		{int64(4), "Charlie"},
		{int64(5), nil},
	}

	batch := data.Batch{Rows: rows}
	batchChan <- batch
	close(batchChan)

	rg := sync.WaitGroup{}
	rg.Add(1)
	go func() {
		bw := pdw.CreateBatchWriter()
		defer rg.Done()
		ds.BatchesToWriter(ctx, bw)
	}()
	rg.Wait()

	pdw.Close()

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(nil)))
	assert.NoError(t, err)

	idValues, defLevels, err := GetRowGroupColumn[int64](reader, 0)
	assert.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, idValues)
	assert.Equal(t, []int16{1, 1, 1, 1, 1}, defLevels)

	nameValues, defLevels, err := GetRowGroupColumn[string](reader, 1)
	assert.NoError(t, err)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, nameValues)
	assert.Equal(t, []int16{1, 1, 0, 1, 0}, defLevels)

	metadata := reader.MetaData()
	assert.NotNil(t, metadata)
	assert.Equal(t, 2, metadata.Schema.NumColumns())
	assert.Equal(t, 5, metadata.FileMetaData.NumRows)
}
