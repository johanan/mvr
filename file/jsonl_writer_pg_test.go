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
)

func TestJsonlWriter_FromPG(t *testing.T) {
	// Create a buffer to write the CSV data to
	var buf bytes.Buffer

	// actually query the database
	sc := &data.StreamConfig{StreamName: "test_numbers", Format: "csv", SQL: "SELECT * FROM public.numbers"}
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

	// Check the output
	expected := `{"bigint_value":1,"decimal_value":"468797.177024568","integer_value":1,"smallint_value":1}
{"bigint_value":2,"decimal_value":"191886.800531254","integer_value":2,"smallint_value":2}
{"bigint_value":3,"decimal_value":"723041.1654307","integer_value":3,"smallint_value":3}
{"bigint_value":9223372036854775807,"decimal_value":"507531.111989867","integer_value":2147483647,"smallint_value":32767}
`
	if buf.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buf.String())
	}

}
