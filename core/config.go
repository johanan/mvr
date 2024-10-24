package core

import (
	"database/sql"
	"io"
	"net/url"
	"sync"
)

type Connection struct {
	Url       string
	ParsedUrl *url.URL
}

type StreamConfig struct {
	Stream_Name string `json:"stream_name,omitempty" yaml:"stream_name,omitempty"`
	Object      string `json:"object,omitempty" yaml:"object,omitempty"`
	Format      string `json:"format,omitempty" yaml:"format,omitempty"`
	SQL         string `json:"sql,omitempty" yaml:"sql,omitempty"`
	Compression string `json:"compression,omitempty" yaml:"compression,omitempty"`
}

type Config struct {
	Source       string        `json:"source,omitempty" yaml:"source,omitempty"`
	Dest         string        `json:"dest,omitempty" yaml:"dest,omitempty"`
	SourceConn   *Connection   `json:"-" yaml:"-"`
	DestConn     *Connection   `json:"-" yaml:"-"`
	StreamConfig *StreamConfig `json:"stream_config,omitempty" yaml:"stream_config,omitempty"`
}

type DatabaseOptions struct {
	Sql string `json:"sql" yaml:"sql"`
}

type ExecutionConfig struct {
	Config        *Config
	ReaderConn    *sql.DB
	ReaderOptions *DatabaseOptions
	WriterPipe    *io.PipeWriter
	ReaderWg      *sync.WaitGroup
	WriterWg      *sync.WaitGroup
	Concurrency   int
}

func parseConnection(urlString string) *Connection {
	parse, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	return &Connection{Url: urlString, ParsedUrl: parse}
}

func NewConfig(source, dest string, streamConfig *StreamConfig) *Config {
	sourceConn := parseConnection(source)
	destConn := parseConnection(dest)
	return &Config{Source: source, Dest: dest, StreamConfig: streamConfig, SourceConn: sourceConn, DestConn: destConn}
}

func NewExecutionConfig(config *Config, concurrency int) *ExecutionConfig {
	sql := "SELECT * FROM " + config.StreamConfig.Stream_Name
	dbOptions := &DatabaseOptions{Sql: sql}

	return &ExecutionConfig{
		Config:        config,
		ReaderConn:    nil,
		ReaderOptions: dbOptions,
		WriterPipe:    nil,
		ReaderWg:      &sync.WaitGroup{},
		WriterWg:      &sync.WaitGroup{},
		Concurrency:   concurrency,
	}
}
