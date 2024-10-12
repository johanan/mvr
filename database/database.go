package database

import (
	"database/sql"
	"log"
	"net/url"

	"github.com/johanan/mvr/data"
	_ "github.com/lib/pq"
)

type Column = data.Column
type Batch = data.Batch
type DataStream = data.DataStream

func CreateDataStream(connUrl *url.URL, query string) (ds *DataStream, conn *sql.DB) {
	db, err := sql.Open(connUrl.Scheme, connUrl.String())
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	columns, err := GetColumns(db, query)
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	batchChan := make(chan Batch, 10) // Buffered channel with a size of 10

	datastream := &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: 2, Columns: columns}

	return datastream, db
}

func GetColumns(db *sql.DB, query string) ([]Column, error) {
	result, err := db.Query(query + " LIMIT 0 OFFSET 0")
	if err != nil {
		return nil, err
	}
	defer result.Close()

	columns, err := result.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var cols []Column
	for _, col := range columns {
		length, _ := col.Length()
		nullable, _ := col.Nullable()
		cols = append(cols, Column{
			Name:         col.Name(),
			DatabaseType: col.DatabaseTypeName(),
			Length:       length,
			Nullable:     nullable,
			ScanType:     col.ScanType().Name(),
		})
	}
	return cols, nil
}
