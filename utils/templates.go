package utils

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

func ParseTemplate(name, content string) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(sprig.TxtFuncMap()).Parse(content)
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s: %w", name, err)
	}
	return tmpl, nil
}

func ExecuteTemplate(tmpl *template.Template, vars interface{}) ([]byte, error) {
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, vars); err != nil {
		return nil, fmt.Errorf("error executing template: %w", err)
	}
	return rendered.Bytes(), nil
}
