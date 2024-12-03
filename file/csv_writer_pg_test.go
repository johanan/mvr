package file

import (
	"bytes"
	"context"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/johanan/mvr/data"
	"github.com/johanan/mvr/database"
	"github.com/zeebo/assert"
)

func TestCSVWriter_FromPG(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name: "Numbers Test",
			sql:  "SELECT * FROM public.numbers",
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
			sql:  "SELECT * FROM public.strings",
			expected: `char_value,varchar_value,text_value,json_value,jsonb_value,array_value
a         ,a,a,{},{},[]
b         ,b,b,"{""key"":""value""}","{""key"":""value""}","[""a""]"
`,
		},
		{
			name: "Default users Table Test",
			sql:  "SELECT * FROM public.users",
			expected: `name,created,createdz,unique_id,nullable_id,active
John Doe,2024-10-08T17:22:00,2024-10-08T17:22:00Z,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11,NULL,true
Test Tester,2024-10-08T17:22:00,2024-10-08T17:22:00Z,a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12,NULL,false
`,
		},
	}
	os.Setenv("TZ", "UTC")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer

			// actually query the database
			sc := &data.StreamConfig{StreamName: tt.name, Format: "csv", SQL: tt.sql}
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

			writer := NewCSVDataWriter(pgDs, &buf)

			rg.Add(1)
			go pgDs.BatchesToWriter(&rg, writer)
			rg.Wait()

			assert.Equal(t, tt.expected, buf.String())
		})
	}
	os.Unsetenv("TZ")
}
