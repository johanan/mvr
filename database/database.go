package database

import (
	"database/sql"
	"log"
	"net/url"

	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/johanan/mvr/data"
)

type Column = data.Column
type Batch = data.Batch
type DataStream = data.DataStream

func CreateDataStream(connUrl *url.URL, query string) (ds *DataStream, conn *sql.DB) {
	scheme := connUrl.Scheme
	if scheme == "postgres" {
		scheme = "pgx"
	}

	db, err := sql.Open(scheme, connUrl.String())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	columns, err := GetColumns(db, query)
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	batchChan := make(chan Batch, 10)

	datastream := &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: 1000, Columns: columns}

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
	for i, col := range columns {
		length, _ := col.Length()
		nullable, _ := col.Nullable()
		scale, precision, _ := col.DecimalSize()
		cols = append(cols, Column{
			Name:         col.Name(),
			DatabaseType: col.DatabaseTypeName(),
			Length:       length,
			Nullable:     nullable,
			ScanType:     col.ScanType().Name(),
			Position:     i,
			Scale:        scale,
			Precision:    precision,
		})
	}
	return cols, nil
}
