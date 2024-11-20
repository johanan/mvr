package file

import (
	"bytes"
	"context"
	"log"
	"net/url"
	"sync"
	"testing"

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
			expected: `{"bigint_value":1,"decimal_value":"468797.177024568","integer_value":1,"smallint_value":1}
{"bigint_value":2,"decimal_value":"191886.800531254","integer_value":2,"smallint_value":2}
{"bigint_value":3,"decimal_value":"723041.1654307","integer_value":3,"smallint_value":3}
{"bigint_value":9223372036854775807,"decimal_value":"507531.111989867","integer_value":2147483647,"smallint_value":32767}
`,
		},
		{
			name: "Strings Test",
			sql:  "SELECT * FROM public.strings",
			expected: `{"array_value":[],"char_value":"a         ","json_value":{},"jsonb_value":{},"text_value":"a","varchar_value":"a"}
{"array_value":["a"],"char_value":"b         ","json_value":{"key":"value"},"jsonb_value":{"key":"value"},"text_value":"b","varchar_value":"b"}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to write the CSV data to
			var buf bytes.Buffer

			// actually query the database
			sc := &data.StreamConfig{StreamName: tt.name, Format: "jsonl", SQL: tt.sql}
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

			writer := &JSONLWriter{pgDs, &buf}

			rg.Add(1)
			go pgDs.BatchesToWriter(&rg, writer)
			rg.Wait()

			assert.Equal(t, tt.expected, buf.String())
		})
	}

}
