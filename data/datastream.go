package data

import (
	"context"
	"log"
	"maps"
	"net/url"
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v2"
)

type StreamConfig struct {
	StreamName  string                 `json:"stream_name,omitempty" yaml:"stream_name,omitempty"`
	Filename    *string                `json:"filename,omitempty" yaml:"filename,omitempty"`
	Format      string                 `json:"format,omitempty" yaml:"format,omitempty"`
	SQL         string                 `json:"sql,omitempty" yaml:"sql,omitempty"`
	Compression string                 `json:"compression,omitempty" yaml:"compression,omitempty"`
	Columns     []Column               `json:"columns,omitempty" yaml:"columns,omitempty"`
	Params      map[string]interface{} `json:"params,omitempty" yaml:"params,omitempty"`
}

type Column struct {
	Name         string `json:"name" yaml:"name"`
	DatabaseType string `json:"database_type" yaml:"database_type"`
	Length       int64  `json:"length,omitempty" yaml:"length,omitempty"`
	Nullable     bool   `json:"nullable" yaml:"nullable"`
	ScanType     string `json:"scan_type" yaml:"scan_type"`
	Position     int    `json:"position" yaml:"position"`
	Scale        int64  `json:"scale,omitempty" yaml:"scale,omitempty"`
	Precision    int64  `json:"precision,omitempty" yaml:"precision,omitempty"`
}

type Batch struct {
	Rows [][]any
}

type DataStream struct {
	TotalRows   int
	Mux         sync.Mutex
	BatchChan   chan Batch
	BatchSize   int
	Columns     []Column
	DestColumns []Column
}

type DataWriter interface {
	WriteRow(row []any) error
	Flush() error
	Close() error
}

type DBReaderConn interface {
	CreateDataStream(cs *url.URL, config *StreamConfig) (*DataStream, error)
	ExecuteDataStream(ctx context.Context, ds *DataStream, config *StreamConfig) error
	Close() error
}

func NewStreamConfigFromYaml(data []byte) (*StreamConfig, error) {
	var streamConfig StreamConfig
	err := yaml.Unmarshal(data, &streamConfig)
	if err != nil {
		return nil, err
	}

	if streamConfig.SQL == "" {
		streamConfig.SQL = "SELECT * FROM " + streamConfig.StreamName
	}

	env_params := GetMVRVars("PARAM")
	if streamConfig.Params == nil {
		streamConfig.Params = make(map[string]interface{})
	}

	maps.Insert(streamConfig.Params, maps.All(env_params))

	return &streamConfig, nil
}

func GetMVRVars(prefix string) map[string]any {
	full_prefix := "MVR_" + prefix + "_"
	params := make(map[string]any)
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, full_prefix) {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimPrefix(parts[0], full_prefix)
				params[key] = parts[1]
			}
		}
	}
	return params
}

func (ds *DataStream) BatchesToWriter(wg *sync.WaitGroup, writer DataWriter) {
	defer wg.Done()

	for batch := range ds.BatchChan {
		ds.Mux.Lock()
		for _, row := range batch.Rows {
			err := writer.WriteRow(row)
			ds.TotalRows++
			if err != nil {
				log.Fatalf("Failed to write row: %v", err)
			}
		}

		err := writer.Flush()
		if err != nil {
			log.Fatalf("Failed to flush writer: %v", err)
		}

		ds.Mux.Unlock()
	}

}

func OverrideColumns(original []Column, overrides []Column) []Column {
	overrideMap := make(map[string]Column)
	for _, col := range overrides {
		overrideMap[strings.ToLower(col.Name)] = col
	}

	for i, col := range original {
		if overrideCol, found := overrideMap[strings.ToLower(col.Name)]; found {
			if overrideCol.DatabaseType != "" {
				original[i].DatabaseType = strings.ToUpper(overrideCol.DatabaseType)
			}
			if overrideCol.Length != 0 {
				original[i].Length = overrideCol.Length
			}
			if overrideCol.Nullable {
				original[i].Nullable = overrideCol.Nullable
			}
			if overrideCol.Position != 0 {
				original[i].Position = overrideCol.Position
			}
			if overrideCol.Scale != 0 {
				original[i].Scale = overrideCol.Scale
			}
			if overrideCol.Precision != 0 {
				original[i].Precision = overrideCol.Precision
			}
		}
	}

	return original
}
