package core

import (
	"net/url"
	"time"

	"github.com/johanan/mvr/data"
	"github.com/rs/zerolog"
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

type FlowResult struct {
	source       string
	sql          string
	path         string
	rows         int
	start        time.Time
	elapsed      time.Duration
	elapsedHuman string
	bytes        float64
	success      bool
	error        string
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

func NewFlowResult(source *url.URL, stream *data.StreamConfig, start time.Time) *FlowResult {
	cleaned := *source
	cleaned.User = nil
	result := &FlowResult{source: cleaned.String(), sql: stream.SQL, start: start}
	return result
}

func (fr *FlowResult) SetPath(path *url.URL) *FlowResult {
	cleaned := *path
	cleaned.User = nil
	fr.path = cleaned.String()
	return fr
}

func (fr *FlowResult) SetRows(rows int) *FlowResult {
	fr.rows = rows
	return fr
}

func (fr *FlowResult) SetBytes(bytes float64) *FlowResult {
	fr.bytes = bytes
	return fr
}

func (fr *FlowResult) Success() *FlowResult {
	fr.success = true
	elapsed := time.Since(fr.start)
	fr.elapsed = elapsed
	fr.elapsedHuman = elapsed.String()
	return fr
}

func (fr *FlowResult) Error(err string) *FlowResult {
	fr.success = false
	fr.error = err
	elapsed := time.Since(fr.start)
	fr.elapsed = elapsed
	fr.elapsedHuman = elapsed.String()
	return fr
}

func (fr *FlowResult) LogContext(zLog *zerolog.Event) *zerolog.Event {
	return zLog.
		Str("source", fr.source).
		Str("sql", fr.sql).
		Str("path", fr.path).
		Int("rows", fr.rows).
		Dur("elapsed", fr.elapsed).
		Str("duration", fr.elapsedHuman).
		Float64("bytes", fr.bytes).
		Bool("success", fr.success).
		Str("error", fr.error)
}
