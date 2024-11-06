package database

import (
	"log"
	"net/url"
	"strings"

	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/johanan/mvr/data"
	_ "github.com/snowflakedb/gosnowflake"
)

type Column = data.Column
type Batch = data.Batch
type DataStream = data.DataStream

func CreateDataStream(connUrl *url.URL, config *data.StreamConfig) (ds *DataStream, conn *sqlx.DB) {
	connString := connUrl.String()
	scheme := connUrl.Scheme
	if scheme == "postgres" {
		scheme = "pgx"
	}

	if scheme == "snowflake" {
		connString = strings.ReplaceAll(connString, "snowflake://", "")
	}

	db, err := sqlx.Open(scheme, connString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	columns, err := GetColumns(db, config.SQL, connUrl.Scheme, config.Params)
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	var destColumns []Column
	switch connUrl.Scheme {
	case "postgres":
		destColumns = columns
	case "snowflake":
		destColumns = sfColumnsToPg(columns)
	}

	if len(config.Columns) > 0 {
		destColumns = data.OverrideColumns(destColumns, config.Columns)
	}

	batchChan := make(chan Batch, 10)

	datastream := &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: 1000, Columns: columns, DestColumns: destColumns}

	return datastream, db
}

func GetColumns(db *sqlx.DB, query string, dbType string, arg map[string]interface{}) ([]Column, error) {
	result, err := db.NamedQuery("SELECT * FROM ("+query+") LIMIT 0 OFFSET 0", arg)
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
		precision, scale, _ := col.DecimalSize()
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

func sfColumnsToPg(columns []Column) []Column {
	pgCols := make([]Column, len(columns))
	copy(pgCols, columns)
	for i, col := range pgCols {
		switch col.DatabaseType {
		case "FIXED":
			if col.Scale == 0 {
				switch {
				case col.Precision <= 4:
					pgCols[i].DatabaseType = "INT2" // 2 bytes
				case col.Precision <= 9:
					pgCols[i].DatabaseType = "INT4" // 4 bytes
				default:
					pgCols[i].DatabaseType = "INT8" // 8 bytes
				}
			} else {
				pgCols[i].DatabaseType = "NUMERIC"
				pgCols[i].Precision = col.Precision
				pgCols[i].Scale = col.Scale
			}
		case "TIMESTAMP_NTZ":
			pgCols[i].DatabaseType = "TIMESTAMP"

		case "TIMESTAMP_TZ":
			pgCols[i].DatabaseType = "TIMESTAMPTZ"

		case "ARRAY", "VARIANT":
			pgCols[i].DatabaseType = "JSONB"
		}
	}
	return pgCols
}
