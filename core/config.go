package core

import (
	"io"
	"net/url"
	"sync"

	"github.com/johanan/mvr/data"
)

type Connection struct {
	Url       string
	ParsedUrl *url.URL
}

type Config struct {
	Source       string             `json:"source,omitempty" yaml:"source,omitempty"`
	Dest         string             `json:"dest,omitempty" yaml:"dest,omitempty"`
	SourceConn   *Connection        `json:"-" yaml:"-"`
	DestConn     *Connection        `json:"-" yaml:"-"`
	StreamConfig *data.StreamConfig `json:"stream_config,omitempty" yaml:"stream_config,omitempty"`
}

type ExecutionConfig struct {
	Config      *Config
	WriterPipe  *io.PipeWriter
	ReaderWg    *sync.WaitGroup
	WriterWg    *sync.WaitGroup
	Concurrency int
}

func parseConnection(urlString string) *Connection {
	parse, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	return &Connection{Url: urlString, ParsedUrl: parse}
}

func NewConfig(source, dest string, streamConfig *data.StreamConfig) *Config {
	sourceConn := parseConnection(source)
	destConn := parseConnection(dest)
	return &Config{Source: source, Dest: dest, StreamConfig: streamConfig, SourceConn: sourceConn, DestConn: destConn}
}

func NewExecutionConfig(config *Config, concurrency int) *ExecutionConfig {
	return &ExecutionConfig{
		Config:      config,
		WriterPipe:  nil,
		ReaderWg:    &sync.WaitGroup{},
		WriterWg:    &sync.WaitGroup{},
		Concurrency: concurrency,
	}
}
