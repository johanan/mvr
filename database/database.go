package database

import (
	"database/sql"

	"github.com/johanan/mvr/data"
)

type Column = data.Column
type Batch = data.Batch
type DataStream = data.DataStream

func MapToMvrColumns(columns []*sql.ColumnType) []Column {
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

	return cols
}
