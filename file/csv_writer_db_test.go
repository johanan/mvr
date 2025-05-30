package file

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/johanan/mvr/core"
	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/rs/zerolog"
	"github.com/zeebo/assert"
)

func TestCSVWriter_FromPG(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected string
	}{
		{
			name: "Numbers Test",
			yaml: `stream_name: public.numbers
format: csv`,
			expected: `smallint_value,integer_value,bigint_value,decimal_value,double_value,float_value
1,1,1,1.000000000000000,1,1
2,2,2,2.000000000000000,2,2
3,3,3,3.000000000000000,3,3
32767,2147483647,9223372036854775807,507531.111989867000000,123456789012345680000,12345679
0,0,0,468797.177024568000000,1234567890.12345,12345.67
`,
		},
		{
			name: "Strings Test",
			yaml: `stream_name: public.strings
format: csv`,
			expected: `char_value,varchar_value,text_value,json_value,jsonb_value,array_value
a         ,a,a,{},{},[]
b         ,b,b,"{""key"":""value""}","{""key"":""value""}","[""a""]"
`,
		},
		{
			name: "Default users Table Test",
			yaml: `stream_name: public.users
format: csv`,
			expected: `name,created,createdz,unique_id,nullable_id,active
John Doe,2024-10-08T17:22:00,2024-10-08T17:22:00Z,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL,true
Test Tester,2024-10-08T17:22:00,2024-10-08T17:22:00Z,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12,NULL,false
`,
		},
		{
			name: "Numbers Limit Test",
			yaml: `stream_name: public.numbers
format: csv
sql: SELECT * FROM public.numbers LIMIT $1
params:
  "$1":
    value: 1
    type: INT8`,
			expected: `smallint_value,integer_value,bigint_value,decimal_value,double_value,float_value
1,1,1,1.000000000000000,1,1
`,
		},
		{
			name: "Users With Date Test",
			yaml: `stream_name: public.users
format: csv
sql: SELECT * FROM public.users WHERE createdz < $1
params:
  P1:
    value: "2024-10-07 17:22:0000+0000"`,
			expected: `name,created,createdz,unique_id,nullable_id,active
`,
		},
	}
	os.Setenv("TZ", "UTC")
	loc := time.Local
	time.Local = time.UTC
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer
			writeCloser := NewWriteCloseBuffer(&buf)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			zerolog.SetGlobalLevel(zerolog.Disabled)

			// actually query the database
			sc, err := data.BuildConfig([]byte(tt.yaml), &data.StreamConfig{})
			assert.NoError(t, err)
			err = sc.Validate()
			assert.NoError(t, err)
			local_url, _ := url.Parse(local_db_url)

			pgr, _ := database.NewPGDataReader(local_url)
			pgDs, _ := pgr.CreateDataStream(ctx, local_url, sc)
			defer pgr.Close()

			writer := NewCSVDataWriter(pgDs, writeCloser)

			err = core.Execute(ctx, 1, sc, pgDs, pgr, writer)
			assert.NoError(t, err)
			writer.Close()

			assert.Equal(t, tt.expected, buf.String())
		})
	}
	time.Local = loc
	os.Unsetenv("TZ")
}

func TestCsvWriter_FromMS(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected string
	}{
		{
			name: "Numbers Test",
			yaml: `stream_name: dbo.numbers
format: csv`,
			expected: `smallint_value,integer_value,bigint_value,decimal_value,double_value,float_value
1,1,1,1.000000000000000,1,1
2,2,2,2.000000000000000,2,2
3,3,3,3.000000000000000,3,3
32767,2147483647,9223372036854775807,507531.111989867000000,123456789012345670000,12345679
0,0,0,468797.177024568000000,1234567890.12345,12345.669921875
`,
		},
		{
			name: "Strings Test",
			yaml: `stream_name: dbo.strings
format: csv
columns:
  - name: json_value
    database_type: JSON
  - name: jsonb_value
    database_type: JSON
  - name: array_value
    database_type: JSON`,
			expected: `char_value,varchar_value,text_value,json_value,jsonb_value,array_value
a         ,a,a,{},{},[]
b         ,b,b,"{""key"": ""value""}","{""key"": ""value""}","[""a""]"
`,
		},
		{
			name: "Default users Table Test",
			yaml: `stream_name: dbo.users
format: csv`,
			expected: `name,created,createdz,unique_id,nullable_id,active
John Doe,2024-10-08T17:22:00,2024-10-08T17:22:00Z,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL,true
Test Tester,2024-10-08T17:22:00,2024-10-08T17:22:00Z,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12,NULL,false
`,
		},
		{
			name: "Numbers Limit Test",
			yaml: `stream_name: dbo.numbers
format: csv
sql: SELECT * FROM dbo.numbers where smallint_value = @smallint
params:
  "smallint":
    value: 1
    type: INT8`,
			expected: `smallint_value,integer_value,bigint_value,decimal_value,double_value,float_value
1,1,1,1.000000000000000,1,1
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer
			writeCloser := NewWriteCloseBuffer(&buf)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			zerolog.SetGlobalLevel(zerolog.Disabled)

			// right now only Azure has json types
			// https://learn.microsoft.com/en-us/sql/t-sql/data-types/json-data-type?view=azuresqldb-current
			// so we will override the types here
			sc, err := data.BuildConfig([]byte(tt.yaml), &data.StreamConfig{})
			assert.NoError(t, err)
			err = sc.Validate()
			assert.NoError(t, err)
			local_url, _ := url.Parse(local_ms_url)

			msr, _ := database.NewMSDataReader(local_url)
			msDs, err := msr.CreateDataStream(ctx, local_url, sc)
			assert.NoError(t, err)
			defer msr.Close()

			writer := NewCSVDataWriter(msDs, writeCloser)

			err = core.Execute(ctx, 1, sc, msDs, msr, writer)
			assert.NoError(t, err)
			writer.Close()

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}
