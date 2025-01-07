package database

import (
	"database/sql"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/johanan/mvr/data"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cast"
)

type Column = data.Column
type Batch = data.Batch
type DataStream = data.DataStream

func logColumns(cols, destCols []Column) {
	jsonColumns, err := json.Marshal(cols)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to marshal columns")
	}

	jsonDest, err := json.Marshal(destCols)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to marshal destColumns")
	}

	log.Debug().Str("columns", string(jsonColumns)).Str("destColumns", string(jsonDest)).Msg("Created data stream")
}

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

func getKeyCaseInsensitive(values url.Values, key string) string {
	for k, v := range values {
		if strings.EqualFold(k, key) {
			return v[0]
		}
	}
	return ""
}
