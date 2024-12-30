package data

import (
	"os"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestConfigLoading(t *testing.T) {
	tests := []struct {
		name   string
		yaml   string
		sql    string
		params []string
		values []string
	}{
		{
			yaml:   `stream_name: "test_stream"`,
			params: []string{"$2", "$3", "$4"},
			values: []string{"from_env_second", "from_env_third", "from_env_fourth"},
		},
		{
			name: "Set in YAML",
			yaml: `stream_name: "test_stream"
params:
  "$1":
    value: first_param
    type: TEXT
  "$2":
  "$3":`,
			params: []string{"$1", "$2", "$3", "$4"},
			values: []string{"first_param", "from_env_second", "from_env_third", "from_env_fourth"},
		},
		{
			yaml: `
stream_name: "test_stream"
params:
  "$1": 
    value: different
  "$4": 
    value: fourth_param`,
			params: []string{"$1", "$2", "$3", "$4"},
			values: []string{"different", "from_env_second", "from_env_third", "from_env_fourth"},
		},
	}
	os.Setenv("MVR_PARAM_$2", "from_env_second")
	os.Setenv("MVR_PARAM_$3", "from_env_third")
	os.Setenv("MVR_PARAM_$4", "from_env_fourth")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := BuildConfig([]byte(tt.yaml), &StreamConfig{})
			assert.NoError(t, err)
			assert.Equal(t, tt.params, config.ParamKeys)
			for i, key := range tt.params {
				assert.Equal(t, tt.values[i], config.Params[key].Value)
			}
		})
	}
	os.Unsetenv("MVR_PARAM_$2")
	os.Unsetenv("MVR_PARAM_$3")
	os.Unsetenv("MVR_PARAM_$4")

}

func TestBuildConfig(t *testing.T) {
	tests := []struct {
		name           string
		inputData      []byte
		cliArgs        *StreamConfig
		expectedConfig *StreamConfig
		expectError    bool
		errorMessage   string
	}{
		{
			name: "Valid Template with Overrides",
			inputData: []byte(`
stream_name: ""
format: ""
sql: ""
compression: "gzip"
columns:
  - name: "id"
    database_type: "INT"
  - name: "name"
    database_type: "VARCHAR"
`),
			cliArgs: &StreamConfig{
				StreamName: "OverriddenStream",
				Format:     "jsonl",
				SQL:        "SELECT id, name FROM users",
			},
			expectedConfig: &StreamConfig{
				StreamName:  "OverriddenStream",
				Format:      "jsonl",
				SQL:         "SELECT id, name FROM users",
				Compression: "gzip",
				Columns: []Column{
					{Name: "id", DatabaseType: "INT"},
					{Name: "name", DatabaseType: "VARCHAR"},
				},
				// Initialize other fields as needed
			},
			expectError: false,
		},
		{
			name: "Template Parsing Error",
			inputData: []byte(`
stream_name: "{{ .StreamName }}"
format: "{{ .Format }}"
sql: "{{ .SQL }`), // Missing closing braces
			cliArgs: &StreamConfig{
				StreamName: "Stream",
				Format:     "jsonl",
				SQL:        "SELECT * FROM table",
			},
			expectedConfig: nil,
			expectError:    true,
			errorMessage:   "error parsing template",
		},
		{
			name: "Override Only Specific Fields",
			inputData: []byte(`
stream_name: "TemplateStream"
format: "csv"
sql: "SELECT a, b FROM table"
compression: "none"`),
			cliArgs: &StreamConfig{
				Format: "jsonl", // Override only the Format field
			},
			expectedConfig: &StreamConfig{
				StreamName:  "TemplateStream", // From template
				Format:      "jsonl",          // Overridden
				SQL:         "SELECT a, b FROM table",
				Compression: "none",
				Columns:     nil,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := BuildConfig(tt.inputData, tt.cliArgs)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, config)

				// Assert individual fields
				assert.Equal(t, tt.expectedConfig.StreamName, config.StreamName)
				assert.Equal(t, tt.expectedConfig.Format, config.Format)
				assert.Equal(t, tt.expectedConfig.SQL, config.SQL)
				assert.Equal(t, tt.expectedConfig.Compression, config.Compression)
				assert.Equal(t, tt.expectedConfig.Columns, config.Columns)

				// Add more field assertions as necessary
			}
		})
	}
}

func Test_CustomFilename(t *testing.T) {
	tests := []struct {
		name             string
		cliArgs          *StreamConfig
		expectedFilename string
	}{
		{
			name: "Date Based Filename",
			cliArgs: &StreamConfig{
				Filename: "folder/{{YYYY}}/{{MM}}/{{DD}}/file.txt",
			},
			expectedFilename: "folder/" + time.Now().Format("2006/01/02") + "/file.txt",
		},
		{
			name: "Stream Name in Filename",
			cliArgs: &StreamConfig{
				StreamName: "test_stream",
				Filename:   "folder/{{stream_name}}.txt",
			},
			expectedFilename: "folder/test_stream.txt",
		},
		{
			name: "ext for csv",
			cliArgs: &StreamConfig{
				Format:   "csv",
				Filename: "folder/file{{ext}}",
			},
			expectedFilename: "folder/file.csv",
		},
		{
			name: "ext for csv with gzip",
			cliArgs: &StreamConfig{
				Format:      "csv",
				Compression: "gzip",
				Filename:    "folder/file{{ext}}",
			},
			expectedFilename: "folder/file.csv.gz",
		},
		{
			name: "ext for jsonl",
			cliArgs: &StreamConfig{
				Format:   "jsonl",
				Filename: "folder/file{{ext}}",
			},
			expectedFilename: "folder/file.jsonl",
		},
		{
			name: "ext for parquet",
			cliArgs: &StreamConfig{
				Format:   "parquet",
				Filename: "folder/file{{ext}}",
			},
			expectedFilename: "folder/file.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cliArgs.SQL = "SELECT * FROM somewhere"
			config, err := BuildConfig([]byte(""), tt.cliArgs)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedFilename, config.Filename)
		})
	}
}
