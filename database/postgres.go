package database

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johanan/mvr/data"
)

type PGDataReader struct {
	Pool *pgxpool.Pool
}

func NewPGDataReader(connUrl *url.URL) (*PGDataReader, error) {
	connString := connUrl.String()
	scheme := connUrl.Scheme
	if scheme == "postgres" {
		scheme = "pgx"
	}

	if scheme != "pgx" {
		return nil, errors.New("only postgres connections are supported")
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxdecimal.Register(conn.TypeMap())
		return nil
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return &PGDataReader{Pool: dbpool}, nil
}

func (pool *PGDataReader) Close() error {
	pool.Pool.Close()
	return nil
}

func (pool *PGDataReader) CreateDataStream(connUrl *url.URL, config *data.StreamConfig) (ds *DataStream, err error) {
	// switch to sql to get the columns
	// this will use the code from pqx stdlib to map columns to a common interface
	// we can also use pqx for querying the actual data
	db := stdlib.OpenDBFromPool(pool.Pool)

	// let's get the columns
	col_query := "SELECT * FROM (" + config.SQL + ") LIMIT 0 OFFSET 0"

	paramValues := BuildParams(config)
	rows, err := db.QueryContext(context.Background(), col_query, paramValues...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbCols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columns := MapToMvrColumns(dbCols)
	destColumns := columns

	if len(config.Columns) > 0 {
		destColumns = data.OverrideColumns(destColumns, config.Columns)
	}

	batchChan := make(chan Batch, 10)

	return &DataStream{TotalRows: 0, BatchChan: batchChan, BatchSize: 1000, Columns: columns, DestColumns: destColumns}, nil

}

func (pool *PGDataReader) ExecuteDataStream(ctx context.Context, ds *DataStream, config *data.StreamConfig) error {
	paramValues := BuildParams(config)
	rows, err := pool.Pool.Query(ctx, config.SQL, paramValues...)
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
