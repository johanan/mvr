package data

import (
	"os"
	"testing"

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
			sql:    "SELECT * FROM test_stream",
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
			sql:    "SELECT * FROM test_stream",
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
			sql:    "SELECT * FROM test_stream",
			params: []string{"$1", "$2", "$3", "$4"},
			values: []string{"different", "from_env_second", "from_env_third", "from_env_fourth"},
		},
	}
	os.Setenv("MVR_PARAM_$2", "from_env_second")
	os.Setenv("MVR_PARAM_$3", "from_env_third")
	os.Setenv("MVR_PARAM_$4", "from_env_fourth")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewStreamConfigFromYaml([]byte(tt.yaml))
			assert.NoError(t, err)
			assert.Equal(t, tt.sql, config.SQL)
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
