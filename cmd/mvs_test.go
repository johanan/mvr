package cmd

import (
	"testing"

	"github.com/johanan/mvr/data"
	"github.com/zeebo/assert"
)

func TestTableFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		expected []data.StreamConfig
	}{
		{
			name:     "No filter",
			filter:   "",
			expected: []data.StreamConfig{{StreamName: "table1"}, {StreamName: "table2"}},
		},
		{
			name:     "Filter by table name",
			filter:   "table1",
			expected: []data.StreamConfig{{StreamName: "table1"}},
		},
		{
			name:     "Filter for all tables",
			filter:   "table1,table2",
			expected: []data.StreamConfig{{StreamName: "table1"}, {StreamName: "table2"}},
		},
		{
			name:     "Use Table not in list",
			filter:   "table3",
			expected: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tables := []data.StreamConfig{{StreamName: "table1"}, {StreamName: "table2"}}
			actual := filterTables(tables, tt.filter)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
