package data

import (
	"log"
	"sync"
)

type Column struct {
	Name         string `json:"name" yaml:"name"`
	DatabaseType string `json:"database_type" yaml:"database_type"`
	Length       int64  `json:"length,omitempty" yaml:"length,omitempty"`
	Nullable     bool   `json:"nullable" yaml:"nullable"`
	ScanType     string `json:"scan_type" yaml:"scan_type"`
	Position     int    `json:"position" yaml:"position"`
	Scale        int64  `json:"scale,omitempty" yaml:"scale,omitempty"`
	Precision    int64  `json:"precision,omitempty" yaml:"precision,omitempty"`
}

type Batch struct {
	Rows [][]any
}

type DataStream struct {
	TotalRows int
	Mux       sync.Mutex
	BatchChan chan Batch
	BatchSize int
	Columns   []Column
}

type DataWriter interface {
	WriteRow(row []any) error
	Flush() error
	Close() error
}

func (ds *DataStream) BatchesToWriter(wg *sync.WaitGroup, writer DataWriter) {
	defer wg.Done()

	for batch := range ds.BatchChan {
		ds.Mux.Lock()
		for _, row := range batch.Rows {
			err := writer.WriteRow(row)
			ds.TotalRows++
			if err != nil {
				log.Fatalf("Failed to write row: %v", err)
			}
		}

		err := writer.Flush()
		if err != nil {
			log.Fatalf("Failed to flush writer: %v", err)
		}

		ds.Mux.Unlock()
	}

}
