package file

import (
	"bytes"
	"context"
	"encoding/hex"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/file"
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

func Test_FullRoundTrip_ToPG(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "Numbers Test",
			sql:  "SELECT * FROM public.numbers",
		},
		{
			name: "Strings Test",
			sql:  "SELECT * FROM public.strings",
		},
		{
			name: "Default users Table Test",
			sql:  "SELECT * FROM public.users",
		},
	}
	os.Setenv("TZ", "UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer

			// actually query the database
			sc := &data.StreamConfig{StreamName: tt.name, Format: "parquet", SQL: tt.sql}
			local_url, _ := url.Parse(local_db_url)

			pgr, _ := database.NewPGDataReader(local_url)
			pgDs, _ := pgr.CreateDataStream(local_url, sc)
			defer pgr.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			rg := sync.WaitGroup{}

			rg.Add(1)
			go func() {
				defer rg.Done()
				if err := pgr.ExecuteDataStream(ctx, pgDs, sc); err != nil {
					log.Printf("ExecuteDataStream error: %v", err)
					cancel()
				}
			}()

			writer := NewParquetDataWriter(pgDs, &buf)

			rg.Add(1)
			go pgDs.BatchesToWriter(&rg, writer)
			rg.Wait()
			writer.Close()

			// going to have to come back to this
		})
	}
	os.Unsetenv("TZ")
}

func TestParquetWriter(t *testing.T) {
	var buf bytes.Buffer

	batchChan := make(chan data.Batch, 10)
	ds := &data.DataStream{
		BatchChan: batchChan,
		BatchSize: 10,
		Mux:       sync.Mutex{},
		DestColumns: []data.Column{
			{Name: "id", DatabaseType: "INT8"},
			{Name: "name", DatabaseType: "TEXT"},
		},
	}

	pdw := NewParquetDataWriter(ds, &buf)

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
	go ds.BatchesToWriter(&rg, pdw)
	rg.Wait()

	pdw.Close()

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()), file.WithReadProps(parquet.NewReaderProperties(nil)))
	assert.NoError(t, err)

	metadata := reader.MetaData()
	assert.NotNil(t, metadata)
	assert.Equal(t, 2, metadata.Schema.NumColumns())
	assert.Equal(t, 5, metadata.FileMetaData.NumRows)

	cols := metadata.Schema.NumColumns()

	for r := 0; r < reader.NumRowGroups(); r++ {
		rgr := reader.RowGroup(r)
		rgMeta := rgr.MetaData()
		for c := 0; c < cols; c++ {
			var valueBuffer interface{}
			defLevels := make([]int16, rgMeta.NumRows())
			chunkMeta, _ := rgMeta.ColumnChunk(c)
			column, _ := rgr.Column(c)
			stats, _ := chunkMeta.Statistics()
			switch r := column.(type) {
			case *file.Int64ColumnChunkReader:
				valueBuffer = make([]int64, stats.NumValues())
				values := valueBuffer.([]int64)
				r.ReadBatch(5, values, defLevels, nil)
				assert.Equal(t, []int64{1, 2, 3, 4, 5}, values)
				assert.Equal(t, []int16{1, 1, 1, 1, 1}, defLevels)
			case *file.ByteArrayColumnChunkReader:
				valueBuffer = make([]parquet.ByteArray, stats.NumValues())
				values := valueBuffer.([]parquet.ByteArray)
				r.ReadBatch(5, values, defLevels, nil)
				assert.Equal(t, 3, len(values))
				assert.Equal(t, "Alice", string(values[0]))
				assert.Equal(t, []int16{1, 1, 0, 1, 0}, defLevels)
			}
		}

	}

}
