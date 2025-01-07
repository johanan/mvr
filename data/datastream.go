package data

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/rs/zerolog/log"
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
	BatchSize   int `json:"batch_size,omitempty" yaml:"batch_size,omitempty"`
	BatchCount  int `json:"batch_count,omitempty" yaml:"batch_count,omitempty"`
}

type MultiStreamConfig struct {
	StreamConfig `json:",inline" yaml:",inline"`
	Tables       []StreamConfig `json:"tables" yaml:"tables"`
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

type BatchWriter interface {
	WriteBatch(batch Batch) error
}

type DataWriter interface {
	CreateBatchWriter() BatchWriter
	Flush() error
	Close() error
}

type DBReaderConn interface {
	CreateDataStream(ctx context.Context, cs *url.URL, config *StreamConfig) (*DataStream, error)
	ExecuteDataStream(ctx context.Context, ds *DataStream, config *StreamConfig) error
	Close() error
}

func (sc *StreamConfig) GetBatchSize() int {
	if sc.BatchSize == 0 {
		return 100000
	}
	return sc.BatchSize
}

func (sc *StreamConfig) GetBatchCount() int {
	if sc.BatchCount == 0 {
		return 10
	}
	return sc.BatchCount
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

	if cliArgs.BatchSize != 0 {
		sc.BatchSize = cliArgs.BatchSize
	}

	if len(cliArgs.Columns) > 0 {
		sc.Columns = cliArgs.Columns
	}

	if len(cliArgs.Params) > 0 {
		sc.Params = cliArgs.Params
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

type TemplateString struct {
	Raw string
}

func (t *TemplateString) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return unmarshal(&t.Raw)
}

type RawStreamConfig struct {
	Filename  TemplateString `yaml:"filename"`
	BatchSize int            `yaml:"batch_size"`
}

func BuildConfig(data []byte, cliArgs *StreamConfig) (*StreamConfig, error) {
	var rawConfig RawStreamConfig
	if err := yaml.Unmarshal(data, &rawConfig); err != nil {
		return nil, fmt.Errorf("error parsing raw yaml: %v", err)
	}

	first, err := ParseAndExecuteTemplate(data, &StreamConfig{})
	if err != nil {
		return nil, fmt.Errorf("error parsing template: %v", err)
	}

	sConfig, err := NewStreamConfigFromYaml(first)
	if err != nil {
		return nil, fmt.Errorf("error parsing config: %v", err)
	}

	if rawConfig.Filename.Raw != "" {
		sConfig.Filename = rawConfig.Filename.Raw
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

func (ds *DataStream) BatchesToWriter(ctx context.Context, writer BatchWriter) error {
	for batch := range ds.BatchChan {
		select {
		case <-ctx.Done():
			log.Trace().Msg("Context done")
			return ctx.Err()
		default:
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			ds.Mux.Lock()
			ds.TotalRows += len(batch.Rows)
			ds.Mux.Unlock()
		}
		log.Trace().Int("total_rows", ds.TotalRows).Msg("Batch written")
	}

	return nil
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
