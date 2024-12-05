package database

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/johanan/mvr/data"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeDataReader struct {
	Snowflake *sqlx.DB
}

func NewSnowflakeDataReader(connUrl *url.URL) (*SnowflakeDataReader, error) {
	connString := connUrl.String()
	scheme := connUrl.Scheme
	if scheme != "snowflake" {
		return nil, errors.New("only snowflake connections are supported")
	}

	connString = strings.ReplaceAll(connString, "snowflake://", "")

	db, err := sqlx.Open("snowflake", connString)
	if err != nil {
		return nil, err
	}

	return &SnowflakeDataReader{Snowflake: db}, nil
}

func (sf *SnowflakeDataReader) Close() error {
	sf.Snowflake.Close()
	return nil
}

func (sf *SnowflakeDataReader) CreateDataStream(connUrl *url.URL, config *data.StreamConfig) (ds *DataStream, err error) {
	db := sf.Snowflake

	// let's get the columns
	col_query := "SELECT * FROM (" + config.SQL + ") LIMIT 0 OFFSET 0"

	rows, err := db.NamedQuery(col_query, config.Params)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbCols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := MapToMvrColumns(dbCols)
	destColumns := sfColumnsToPg(columns)

	if len(config.Columns) > 0 {
		destColumns = data.OverrideColumns(destColumns, config.Columns)
	}

	batchChan := make(chan Batch, 10)

	return &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: 1000, Columns: columns, DestColumns: destColumns, IsSqlServer: false}, nil
}

func (sf *SnowflakeDataReader) ExecuteDataStream(ctx context.Context, ds *DataStream, config *data.StreamConfig) error {
	db := sf.Snowflake
	stmt, err := db.PrepareNamedContext(gosnowflake.WithHigherPrecision(ctx), config.SQL)
	if err != nil {
		log.Fatalf("Failed to prepare query: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.QueryContext(gosnowflake.WithHigherPrecision(ctx), config.Params)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	batch := Batch{Rows: make([][]any, 0, ds.BatchSize)}

	for result.Next() {
		row := make([]any, len(ds.Columns))
		rowPtrs := make([]any, len(ds.Columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := result.Scan(rowPtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch.Rows = append(batch.Rows, row)

		if len(batch.Rows) >= ds.BatchSize {
			select {
			case ds.BatchChan <- batch:
				batch = data.Batch{Rows: make([][]any, 0, ds.BatchSize)}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Send any remaining rows
	if len(batch.Rows) > 0 {
		select {
		case ds.BatchChan <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	close(ds.BatchChan)

	return nil
}

func sfColumnsToPg(columns []Column) []Column {
	pgCols := make([]Column, len(columns))
	copy(pgCols, columns)
	for i, col := range pgCols {
		switch col.DatabaseType {
		case "FIXED":
			pgCols[i].DatabaseType = "NUMERIC"
			pgCols[i].Precision = col.Precision
			pgCols[i].Scale = col.Scale
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
