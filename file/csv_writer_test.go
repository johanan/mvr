package file

import (
	"bytes"
	"encoding/csv"
	"testing"
	"time"

	"github.com/johanan/mvr/data"
	"github.com/zeebo/assert"
)

func TestCSVDataWriter_WriteRow(t *testing.T) {
	// Create a buffer to write the CSV data to
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Create a sample DataStream
	datastream := &data.DataStream{
		DestColumns: []data.Column{
			{Name: "timestamp", ScanType: "Time", DatabaseType: "TIMESTAMP"},
			{Name: "timestamptz", ScanType: "Time", DatabaseType: "TIMESTAMPTZ"},
			{Name: "name", ScanType: "string", DatabaseType: "TEXT"},
		},
	}

	// Create the CSVDataWriter
	cw := &CSVDataWriter{
		datastream: datastream,
		writer:     writer,
	}

	// Create a sample row
	row := []any{
		time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC),
		"John Doe",
	}

	// Write the row
	err := cw.WriteRow(row)
	assert.NoError(t, err)

	// Flush the writer to ensure all data is written
	cw.Flush()

	// Check the output
	expected := "2023-10-01T12:00:00,2023-10-01T12:00:00Z,John Doe\n"
	assert.Equal(t, expected, buf.String())
}
