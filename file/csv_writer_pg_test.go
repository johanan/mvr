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

func TestCSVWriter_FromPG(t *testing.T) {
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

	writer := NewCSVDataWriter(pgDs, &buf)

	rg.Add(1)
	go pgDs.BatchesToWriter(&rg, writer)
	rg.Wait()

	// Check the output
	expected := `smallint_value,integer_value,bigint_value,decimal_value
1,1,1,468797.177024568000000
2,2,2,191886.800531254000000
3,3,3,723041.165430700000000
32767,2147483647,9223372036854775807,507531.111989867000000
`
	if buf.String() != expected {
		t.Errorf("Expected %s, got %s", expected, buf.String())
	}

}
