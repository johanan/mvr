package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/johanan/mvr/data"
	"github.com/rs/zerolog/log"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeDataReader struct {
	Snowflake *sql.DB
}

func NewSnowflakeDataReader(connUrl *url.URL) (*SnowflakeDataReader, error) {
	connString := connUrl.String()
	scheme := connUrl.Scheme
	if scheme != "snowflake" {
		return nil, errors.New("only snowflake connections are supported")
	}

	connString = strings.ReplaceAll(connString, "snowflake://", "")

	db, err := sql.Open("snowflake", connString)
	if err != nil {
		return nil, err
	}

	return &SnowflakeDataReader{Snowflake: db}, nil
}

func (sf *SnowflakeDataReader) Close() error {
	sf.Snowflake.Close()
	return nil
}

func (sf *SnowflakeDataReader) CreateDataStream(ctx context.Context, connUrl *url.URL, config *data.StreamConfig) (ds *DataStream, err error) {
	db := sf.Snowflake

	// let's get the columns
	col_query := "SELECT * FROM (" + config.SQL + ") LIMIT 0 OFFSET 0"

	stmt, err := db.PrepareContext(ctx, col_query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	paramValues := BuildParams(config)
	rows, err := stmt.QueryContext(ctx, paramValues...)
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
	logColumns(columns, destColumns)
	return &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: config.GetBatchSize(), Columns: columns, DestColumns: destColumns}, nil
}

func (sf *SnowflakeDataReader) ExecuteDataStream(ctx context.Context, ds *DataStream, config *data.StreamConfig) error {
	log.Debug().Str("sql", config.SQL).Msg("Executing query")
	db := sf.Snowflake
	stmt, err := db.PrepareContext(gosnowflake.WithHigherPrecision(ctx), config.SQL)
	if err != nil {
		log.Fatal().Msgf("Failed to prepare query: %v", err)
	}
	defer stmt.Close()

	paramValues := BuildParams(config)
	result, err := stmt.QueryContext(gosnowflake.WithHigherPrecision(ctx), paramValues...)
	if err != nil {
		log.Fatal().Msgf("Failed to execute query: %v", err)
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
			log.Trace().Msg("Sending batch")
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
		log.Trace().Msg("Sending remaining batch")
		select {
		case ds.BatchChan <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	log.Debug().Msg("Finished reading rows")
	close(ds.BatchChan)
	log.Debug().Msg("Closed batch channel")

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
