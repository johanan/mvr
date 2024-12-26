package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/johanan/mvr/data"
	_ "github.com/microsoft/go-mssqldb"
)

type MSDataReader struct {
	Conn             *sql.DB
	KeepOriginalUUID bool
}

func NewMSDataReader(connUrl *url.URL) (*MSDataReader, error) {
	connString := connUrl.String()
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}
	keepUUID := connUrl.Query().Get("KeepOriginalUUID") == "true"
	return &MSDataReader{Conn: db, KeepOriginalUUID: keepUUID}, nil
}

func (reader *MSDataReader) Close() error {
	return reader.Conn.Close()
}

func (reader *MSDataReader) CreateDataStream(ctx context.Context, connUrl *url.URL, config *data.StreamConfig) (*DataStream, error) {
	col_query := "SELECT * FROM (" + config.SQL + ") as sub ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"

	paramValues := BuildParams(config)
	sqlParams := make([]interface{}, 0, len(config.ParamKeys))
	for i, key := range config.ParamKeys {
		sqlParams = append(sqlParams, sql.Named(key, paramValues[i]))
	}
	rows, err := reader.Conn.QueryContext(ctx, col_query, sqlParams...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbCols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := MapToMvrColumns(dbCols)
	destColumns := msColumnsToPg(columns)

	if len(config.Columns) > 0 {
		destColumns = data.OverrideColumns(destColumns, config.Columns)
	}

	batchChan := make(chan Batch, 10)

	return &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: 1000, Columns: columns, DestColumns: destColumns}, nil
}

func (reader *MSDataReader) ExecuteDataStream(ctx context.Context, ds *DataStream, config *data.StreamConfig) error {
	paramValues := BuildParams(config)
	sqlParams := make([]interface{}, 0, len(config.ParamKeys))
	for i, key := range config.ParamKeys {
		sqlParams = append(sqlParams, sql.Named(key, paramValues[i]))
	}
	rows, err := reader.Conn.QueryContext(ctx, config.SQL, sqlParams...)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := Batch{Rows: make([][]any, 0, ds.BatchSize)}

	for rows.Next() {
		row := make([]any, len(ds.Columns))
		rowPtrs := make([]any, len(ds.Columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := rows.Scan(rowPtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// fix MS SQL Server wrong endianness for UUID
		for i, col := range ds.DestColumns {
			if !reader.KeepOriginalUUID && col.DatabaseType == "UUID" {
				// null is fine, do not try to convert
				if row[i] == nil {
					continue
				}
				if byteData, ok := row[i].([]byte); ok && len(byteData) == 16 {
					convertedUUID := data.ConvertSQLServerUUID(byteData)
					row[i] = convertedUUID
				}
			}
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

func msColumnsToPg(columns []Column) []Column {
	pgCols := make([]Column, len(columns))
	copy(pgCols, columns)
	for i, col := range pgCols {
		switch col.DatabaseType {
		case "DECIMAL":
			pgCols[i].DatabaseType = "NUMERIC"
			pgCols[i].Precision = col.Precision
			pgCols[i].Scale = col.Scale
		case "SMALLINT":
			pgCols[i].DatabaseType = "INT2"
		case "INT":
			pgCols[i].DatabaseType = "INT4"
		case "BIGINT":
			pgCols[i].DatabaseType = "INT8"
		case "REAL":
			pgCols[i].DatabaseType = "FLOAT4"
		case "FLOAT":
			pgCols[i].DatabaseType = "FLOAT8"
		case "DATETIMEOFFSET":
			pgCols[i].DatabaseType = "TIMESTAMPTZ"
		case "DATETIME":
			pgCols[i].DatabaseType = "TIMESTAMP"
		case "UNIQUEIDENTIFIER":
			pgCols[i].DatabaseType = "UUID"
		}

	}
	return pgCols
}
