package file

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/zeebo/assert"
)

func TestJsonlWriter_FromPG(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name: "Numbers Test",
			sql:  "SELECT * FROM public.numbers",
			expected: `{"bigint_value":1,"decimal_value":"1.000000000000000","double_value":1,"float_value":1,"integer_value":1,"smallint_value":1}
{"bigint_value":2,"decimal_value":"2.000000000000000","double_value":2,"float_value":2,"integer_value":2,"smallint_value":2}
{"bigint_value":3,"decimal_value":"3.000000000000000","double_value":3,"float_value":3,"integer_value":3,"smallint_value":3}
{"bigint_value":9223372036854775807,"decimal_value":"507531.111989867000000","double_value":123456789012345680000,"float_value":12345679,"integer_value":2147483647,"smallint_value":32767}
{"bigint_value":0,"decimal_value":"468797.177024568000000","double_value":1234567890.12345,"float_value":12345.67,"integer_value":0,"smallint_value":0}
`,
		},
		{
			name: "Strings Test",
			sql:  "SELECT * FROM public.strings",
			expected: `{"array_value":[],"char_value":"a         ","json_value":{},"jsonb_value":{},"text_value":"a","varchar_value":"a"}
{"array_value":["a"],"char_value":"b         ","json_value":{"key":"value"},"jsonb_value":{"key":"value"},"text_value":"b","varchar_value":"b"}
`,
		},
		{
			name: "Default users Table Test",
			sql:  "SELECT * FROM public.users",
			expected: `{"active":true,"created":"2024-10-08T17:22:00","createdz":"2024-10-08T17:22:00Z","name":"John Doe","nullable_id":null,"unique_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}
{"active":false,"created":"2024-10-08T17:22:00","createdz":"2024-10-08T17:22:00Z","name":"Test Tester","nullable_id":null,"unique_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"}
`,
		},
	}
	os.Setenv("TZ", "UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// actually query the database
			sc := &data.StreamConfig{StreamName: tt.name, Format: "jsonl", SQL: tt.sql}
			local_url, _ := url.Parse(local_db_url)

			pgr, _ := database.NewPGDataReader(local_url)
			pgDs, _ := pgr.CreateDataStream(ctx, local_url, sc)
			defer pgr.Close()

			writer := NewJSONLWriter(pgDs, &buf)

			err := core.Execute(ctx, 1, sc, pgDs, pgr, writer)
			assert.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
	os.Unsetenv("TZ")
}

// SQL Server handles numbers worse than Postgres, so the expected output is different
func TestJsonlWriter_FromMS(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name: "Numbers Test",
			sql:  "SELECT * FROM dbo.numbers",
			expected: `{"bigint_value":1,"decimal_value":"1.000000000000000","double_value":1,"float_value":1,"integer_value":1,"smallint_value":1}
{"bigint_value":2,"decimal_value":"2.000000000000000","double_value":2,"float_value":2,"integer_value":2,"smallint_value":2}
{"bigint_value":3,"decimal_value":"3.000000000000000","double_value":3,"float_value":3,"integer_value":3,"smallint_value":3}
{"bigint_value":9223372036854775807,"decimal_value":"507531.111989867000000","double_value":123456789012345670000,"float_value":12345679,"integer_value":2147483647,"smallint_value":32767}
{"bigint_value":0,"decimal_value":"468797.177024568000000","double_value":1234567890.12345,"float_value":12345.669921875,"integer_value":0,"smallint_value":0}
`,
		},
		{
			name: "Strings Test",
			sql:  "SELECT * FROM dbo.strings",
			expected: `{"array_value":[],"char_value":"a         ","json_value":{},"jsonb_value":{},"text_value":"a","varchar_value":"a"}
{"array_value":["a"],"char_value":"b         ","json_value":{"key":"value"},"jsonb_value":{"key":"value"},"text_value":"b","varchar_value":"b"}
`,
		},
		{
			name: "Default users Table Test",
			sql:  "SELECT * FROM dbo.users",
			expected: `{"active":true,"created":"2024-10-08T17:22:00","createdz":"2024-10-08T17:22:00Z","name":"John Doe","nullable_id":null,"unique_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}
{"active":false,"created":"2024-10-08T17:22:00","createdz":"2024-10-08T17:22:00Z","name":"Test Tester","nullable_id":null,"unique_id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			override := []data.Column{
				{Name: "json_value", Type: "JSON"},
				{Name: "jsonb_value", Type: "JSON"},
				{Name: "array_value", Type: "JSON"},
			}

			// right now only Azure has json types
			// https://learn.microsoft.com/en-us/sql/t-sql/data-types/json-data-type?view=azuresqldb-current
			// so we will override the types here
			sc := &data.StreamConfig{StreamName: tt.name, Format: "jsonl", SQL: tt.sql, Columns: override}
			local_url, _ := url.Parse(local_ms_url)

			msr, _ := database.NewMSDataReader(local_url)
			msDs, err := msr.CreateDataStream(ctx, local_url, sc)
			assert.NoError(t, err)
			defer msr.Close()

			writer := NewJSONLWriter(msDs, &buf)

			err = core.Execute(ctx, 1, sc, msDs, msr, writer)
			assert.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}
