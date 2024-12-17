package database

import (
	"database/sql"

	"github.com/johanan/mvr/data"
	"github.com/spf13/cast"
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

func BuildParams(config *data.StreamConfig) []interface{} {
	sqlParams := make([]interface{}, 0, len(config.ParamKeys))
	for _, key := range config.ParamKeys {
		pv := config.Params[key]
		switch pv.Type {
		case "TEXT", "":
			sqlParams = append(sqlParams, pv.Value)
		case "INT2", "INT4", "INT8":
			sqlParams = append(sqlParams, cast.ToInt64(pv.Value))
		case "BOOLEAN":
			sqlParams = append(sqlParams, cast.ToBool(pv.Value))
		}

	}
	return sqlParams
}
