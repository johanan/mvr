package cmd

import (
	"fmt"
	"testing"
)

func TestColumnParsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		is_error bool
	}{
		{
			name: "Single column",
			input: `
- name: column1
  type: TEXT`,
			is_error: false,
		},
		{
			name: "JSON format",
			input: `[
  {"name": "column1", "type": "TEXT"},
  {"name": "column2", "type": "BIGINT"}
]`,
			is_error: false,
		},
		{
			name: "Invalid JSON",
			input: `[
			  {"name": "column1", "type": "TEXT"},
			  {"name": "column2", "type": "BIGINT"
			]`,
			is_error: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseColumns([]byte(tt.input))
			if tt.is_error {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			}
			fmt.Println("result: ", result)
		})
	}
}
