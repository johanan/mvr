package core

import (
	"net/url"
	"testing"
	"time"

	"github.com/johanan/mvr/data"
	"github.com/zeebo/assert"
)

func TestFlowResult(t *testing.T) {
	sensitive_url := "postgres://user:password@localhost:5432/dbname?secret_key=sensitive"
	parsedUrl, _ := url.Parse(sensitive_url)
	start := time.Now()
	sc := &data.StreamConfig{SQL: "SELECT * FROM table"}
	flowResult := NewFlowResult(parsedUrl, sc, start)
	assert.NotNil(t, flowResult)
	assert.Equal(t, "postgres://localhost:5432/dbname", flowResult.source)
}
