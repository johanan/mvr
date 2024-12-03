package file

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/parquet-go/parquet-go"
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

type NumbersTest struct {
	SmallintValue int16           `json:"smallint_value" parquet:"name=smallint_value, type=INT32"`
	IntegerValue  int32           `json:"integer_value" parquet:"name=integer_value, type=INT32"`
	BigintValue   int64           `json:"bigint_value" parquet:"name=bigint_value, type=INT64"`
	DecimalValue  decimal.Decimal `json:"decimal_value" parquet:"name=decimal_value, type=, convertedtype=DECIMAL"`
	DoubleValue   float64         `json:"double_value" parquet:"name=double_value, type=DOUBLE"`
	FloatValue    float32         `json:"float_value" parquet:"name=float_value, type=DOUBLE"`
}

type StringsTest struct {
	CharValue    string   `json:"char_value" parquet:"name=char_value, type=BYTE_ARRAY, convertedtype=UTF8"`
	VarcharValue string   `json:"varchar_value" parquet:"name=varchar_value, type=BYTE_ARRAY, convertedtype=UTF8"`
	TextValue    string   `json:"text_value" parquet:"name=text_value, type=BYTE_ARRAY, convertedtype=UTF8"`
	JsonValue    string   `json:"json_value" parquet:"name=json_value, type=BYTE_ARRAY, convertedtype=UTF8"`
	JsonbValue   string   `json:"jsonb_value" parquet:"name=jsonb_value, type=BYTE_ARRAY, convertedtype=UTF8"`
	ArrayValue   []string `json:"array_value" parquet:"name=array_value, type=LIST"`
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

			readBuf := bytes.NewReader(buf.Bytes())
			pr := parquet.NewGenericReader[NumbersTest](readBuf)

			_, err := ReadParquetData(pr)
			assert.NoError(t, err)
			// going to have to come back to this
		})
	}
	os.Unsetenv("TZ")
}

// ReadParquetData reads all rows from a Parquet reader into a slice of structs.
func ReadParquetData[T any](pr *parquet.GenericReader[T]) ([]T, error) {
	n := pr.NumRows()
	rows := make([]T, n)
	_, err := pr.Read(rows)
	if err != nil {
		if err == io.EOF {
			return rows, nil
		}
		return nil, err
	}
	return rows, nil
}
