package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/johanan/mvr/data"
	"github.com/rs/zerolog/log"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeDataReader struct {
	Snowflake *sql.DB
}

func NewSnowflakeDataReader(connUrl *url.URL) (*SnowflakeDataReader, error) {
	scheme := connUrl.Scheme
	if scheme != "snowflake" {
		return nil, errors.New("only snowflake connections are supported")
	}

	privateKey := os.Getenv("MVR_PEM_KEY")
	key_file := os.Getenv("MVR_PEM_FILE")

	if privateKey == "" && key_file != "" {
		fileBytes, err := os.ReadFile(key_file)
		if err != nil {
			return nil, err
		}
		privateKey = string(fileBytes)
	}

	if privateKey != "" {
		key, err := data.ParsePEMPrivateKey(privateKey)
		if err != nil {
			return nil, err
		}
		q := connUrl.Query()
		q.Set("privateKey", data.GeneratePKCS8StringSupress(key))
		q.Set("authenticator", "SNOWFLAKE_JWT")
		connUrl.RawQuery = q.Encode()
	}

	connString := connUrl.String()
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
	log.Debug().Str("sql", col_query).Msg("Getting columns")

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
	srcColumns := sfColumnsToPg(columns)

	if len(config.Columns) > 0 {
		destColumns = data.OverrideColumns(destColumns, config.Columns)
	}

	batchChan := make(chan Batch, config.GetBatchCount())
	logColumns(columns, destColumns)
	return &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: config.GetBatchSize(), Columns: srcColumns, DestColumns: destColumns}, nil
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
	defer func() {
		close(ds.BatchChan)
		log.Debug().Msg("Closed batch channel")
	}()

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
	return nil
}

func (sf *SnowflakeDataReader) ExecuteCommand(ctx context.Context, sql_command string) (string, error) {
	db := sf.Snowflake
	stmt, err := db.PrepareContext(gosnowflake.WithHigherPrecision(ctx), sql_command)
	if err != nil {
		return "", fmt.Errorf("failed to prepare command: %w", err)
	}
	defer stmt.Close()
	var status string
	err = stmt.QueryRowContext(ctx, nil).Scan(&status)

	return status, nil
}

func sfColumnsToPg(columns []Column) []Column {
	pgCols := make([]Column, len(columns))
	copy(pgCols, columns)
	for i, col := range pgCols {
		pgCols[i].Type = col.DatabaseType
		switch col.DatabaseType {
		case "FIXED":
			pgCols[i].Type = "NUMERIC"
			pgCols[i].Precision = col.Precision
			pgCols[i].Scale = col.Scale
		case "TIMESTAMP_NTZ", "TIMESTAMP_LTZ":
			pgCols[i].Type = "TIMESTAMP"

		case "TIMESTAMP_TZ":
			pgCols[i].Type = "TIMESTAMPTZ"

		case "ARRAY", "VARIANT":
			pgCols[i].Type = "JSONB"
		case "TEXT":
			// this is the max size of a text field in snowflake
			// anything under this will be a varchar
			if col.Length < 16777216 {
				pgCols[i].Type = "VARCHAR"
			} else {
				pgCols[i].Type = "TEXT"
				col.Length = -1
			}
		}
	}
	return pgCols
}
