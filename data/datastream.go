package data

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/sprig/v3"
	"gopkg.in/yaml.v2"
)

type StreamConfig struct {
	StreamName  string           `json:"stream_name,omitempty" yaml:"stream_name,omitempty"`
	Filename    string           `json:"filename,omitempty" yaml:"filename,omitempty"`
	Format      string           `json:"format,omitempty" yaml:"format,omitempty"`
	SQL         string           `json:"sql,omitempty" yaml:"sql,omitempty"`
	Compression string           `json:"compression,omitempty" yaml:"compression,omitempty"`
	Columns     []Column         `json:"columns,omitempty" yaml:"columns,omitempty"`
	Params      map[string]Param `json:"params,omitempty" yaml:"params,omitempty"`
	ParamKeys   []string
}

type Param struct {
	Value string `json:"value" yaml:"value"`
	Type  string `json:"type" yaml:"type"`
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
	CreateDataStream(ctx context.Context, cs *url.URL, config *StreamConfig) (*DataStream, error)
	ExecuteDataStream(ctx context.Context, ds *DataStream, config *StreamConfig) error
	Close() error
}

func (sc *StreamConfig) Validate() error {
	if sc.StreamName == "" && sc.SQL == "" {
		return errors.New("stream_name or sql must be provided")
	}

	if sc.SQL == "" {
		sc.SQL = "SELECT * FROM " + sc.StreamName
	}

	return nil
}

func (sc *StreamConfig) OverrideValues(cliArgs *StreamConfig) {
	if cliArgs.StreamName != "" {
		sc.StreamName = cliArgs.StreamName
	}

	if cliArgs.Format != "" {
		sc.Format = cliArgs.Format
	}

	if cliArgs.Filename != "" {
		sc.Filename = cliArgs.Filename
	}

	if cliArgs.SQL != "" {
		sc.SQL = cliArgs.SQL
	}

	if cliArgs.Compression != "" {
		sc.Compression = cliArgs.Compression
	}
}

func ParseAndExecuteTemplate(data []byte, config *StreamConfig) ([]byte, error) {
	tmpl, err := template.New("base").Funcs(sprig.TxtFuncMap()).Funcs(filenameFuncs(config)).Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("error parsing template: %v", err)
	}

	tmplVars := GetMVRVars("TMPL")

	var renderedConfig bytes.Buffer
	tmpl.Execute(&renderedConfig, tmplVars)

	return renderedConfig.Bytes(), nil
}

func ext(config *StreamConfig) string {
	var ext string
	switch strings.ToLower(config.Format) {
	case "csv":
		ext = ".csv"
	case "jsonl":
		ext = ".jsonl"
	case "parquet":
		ext = ".parquet"
	default:
		ext = ""
	}
	switch strings.ToLower(config.Compression) {
	case "gzip":
		ext += ".gz"
	case "snappy":
		ext = ".snappy" + ext
	}

	return ext
}

func filenameFuncs(config *StreamConfig) template.FuncMap {
	return template.FuncMap{
		"YYYY":        func() string { return time.Now().Format("2006") },
		"MM":          func() string { return time.Now().Format("01") },
		"DD":          func() string { return time.Now().Format("02") },
		"stream_name": func() string { return config.StreamName },
		"format":      func() string { return config.Format },
		"compression": func() string { return config.Compression },
		"ext":         func() string { return ext(config) },
	}
}

func NewStreamConfigFromYaml(data []byte) (*StreamConfig, error) {
	var streamConfig StreamConfig
	err := yaml.Unmarshal(data, &streamConfig)
	if err != nil {
		return nil, err
	}

	env_params_raw := GetMVRVars("PARAM")
	env_params := make(map[string]Param)

	for key, value := range env_params_raw {
		env_params[key] = Param{Value: value}
	}
	if streamConfig.Params == nil {
		streamConfig.Params = make(map[string]Param)
	}

	for key, value := range env_params {
		// look for the value in the environment
		if p, found := streamConfig.Params[key]; found {
			p.Value = value.Value
			if p.Type == "" {
				p.Type = "TEXT"
			}
			streamConfig.Params[key] = p
		} else {
			value.Type = "TEXT"
			streamConfig.Params[key] = value
		}
	}

	keys := make([]string, 0, len(streamConfig.Params))
	for key := range streamConfig.Params {
		keys = append(keys, key)
	}
	SortKeys(keys)
	streamConfig.ParamKeys = keys

	return &streamConfig, nil
}

func BuildConfig(data []byte, cliArgs *StreamConfig) (*StreamConfig, error) {
	first, err := ParseAndExecuteTemplate(data, &StreamConfig{})
	if err != nil {
		return nil, fmt.Errorf("error parsing template: %v", err)
	}

	sConfig, err := NewStreamConfigFromYaml(first)
	if err != nil {
		return nil, fmt.Errorf("error parsing config: %v", err)
	}
	sConfig.OverrideValues(cliArgs)
	// create a yaml representation of the config
	data, err = yaml.Marshal(sConfig)
	if err != nil {
		return nil, fmt.Errorf("error marshalling config: %v", err)
	}

	// execute a second time with values from the first parse
	config, err := ParseAndExecuteTemplate(data, sConfig)
	if err != nil {
		return nil, fmt.Errorf("error parsing template: %v", err)
	}

	sConfig, err = NewStreamConfigFromYaml(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing config: %v", err)
	}

	err = sConfig.Validate()
	if err != nil {
		return nil, fmt.Errorf("error validating config: %v", err)
	}

	return sConfig, nil
}

func GetMVRVars(prefix string) map[string]string {
	full_prefix := "MVR_" + prefix + "_"
	params := make(map[string]string)
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

func SortKeys(params []string) {
	slices.SortFunc(params, func(i, j string) int {
		return strings.Compare(i, j)
	})
}

func (ds *DataStream) BatchesToWriter(writer DataWriter) {
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
