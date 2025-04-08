package file

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/zeebo/assert"
)

func TestFullRoundTrip(t *testing.T) {
	tests := []struct {
		name         string
		config       *data.StreamConfig
		expected     interface{}
		nullExpected []bool
	}{
		{
			name:         "Users Name",
			config:       &data.StreamConfig{SQL: "SELECT name FROM public.users", Format: "arrow"},
			expected:     []string{"John Doe", "Test Tester"},
			nullExpected: []bool{false, false},
		},
		{
			name:         "Created Timestamp",
			config:       &data.StreamConfig{SQL: "SELECT created FROM public.users", Format: "arrow"},
			expected:     []arrow.Timestamp{1728408120000000, 1728408120000000},
			nullExpected: []bool{false, false},
		},
		{
			name:         "Timestamp with timezone",
			config:       &data.StreamConfig{SQL: "SELECT createdz FROM public.users", Format: "arrow"},
			expected:     []arrow.Timestamp{1728408120000000, 1728408120000000},
			nullExpected: []bool{false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeCloser := NewWriteCloseBuffer(&buf)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			local_url, _ := url.Parse(local_db_url)

			pgr, _ := database.NewPGDataReader(local_url)
			pgDs, _ := pgr.CreateDataStream(ctx, local_url, tt.config)

			writer := NewArrowDataWriter(pgDs, writeCloser)

			err := core.Execute(ctx, 1, tt.config, pgDs, pgr, writer)
			assert.NoError(t, err)
			writer.Close()

			reader, err := ipc.NewReader(bytes.NewReader(buf.Bytes()))
			assert.NoError(t, err)
			defer reader.Release()

			schema := reader.Schema()
			t.Logf("Schema has %d fields", len(schema.Fields()))
			for i, field := range schema.Fields() {
				t.Logf("Field %d: %s (%s)", i, field.Name, field.Type)
			}

			switch expected := tt.expected.(type) {
			case []int16:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case []int32:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case []int64:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case []float32:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case []float64:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case []string:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case []bool:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			case [][]byte:
				testArrowColumn(t, reader, 0, expected, tt.nullExpected)
			}
		})
	}
}

type arrowTypes interface {
	~int16 | ~int32 | ~int64 | ~float32 | ~float64 | ~string | ~bool | ~[]byte
}

func testArrowColumn[T arrowTypes](t *testing.T, reader *ipc.Reader, colIndex int, expected []T, nullExpected []bool) {
	values, nulls, err := GetArrowColumnValues[T](reader, colIndex)
	assert.NoError(t, err)

	// First check nulls match
	if nullExpected != nil {
		assert.Equal(t, len(nullExpected), len(nulls))
		for i := range nullExpected {
			assert.Equal(t, nullExpected[i], nulls[i])
		}
	}

	// Then check values match (non-null only)
	assert.Equal(t, len(expected), len(values))
	for i := range expected {
		assert.Equal(t, expected[i], values[i])
	}
}

func GetArrowColumnValues[T arrowTypes](reader *ipc.Reader, colIndex int) ([]T, []bool, error) {
	// Validate that reader has records
	if !reader.Next() {
		return nil, nil, nil // No records found
	}

	// Get the record
	record := reader.Record()
	defer record.Release()

	// Get the column
	col := record.Column(colIndex)
	numRows := int(col.Len())

	// Create result slices
	values := make([]T, 0, numRows)
	nulls := make([]bool, numRows)

	// Extract values based on type
	switch ar := col.(type) {
	case *array.Int16:
		if _, ok := any(*new(T)).(int16); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Int32:
		if _, ok := any(*new(T)).(int32); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Int64:
		if _, ok := any(*new(T)).(int64); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Float32:
		if _, ok := any(*new(T)).(float32); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Float64:
		if _, ok := any(*new(T)).(float64); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.String:
		if _, ok := any(*new(T)).(string); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Boolean:
		if _, ok := any(*new(T)).(bool); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.FixedSizeBinary:
		if _, ok := any(*new(T)).([]byte); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Binary:
		if _, ok := any(*new(T)).([]byte); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	case *array.Timestamp:
		if _, ok := any(*new(T)).(int64); !ok {
			break
		}
		typedValues := make([]T, 0, numRows)
		for i := 0; i < numRows; i++ {
			if ar.IsNull(i) {
				nulls[i] = true
			} else {
				typedValues = append(typedValues, any(ar.Value(i)).(T))
			}
		}
		values = typedValues

	default:
		// Handle unsupported types
	}

	return values, nulls, nil
}
