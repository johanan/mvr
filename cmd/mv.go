package cmd

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/johanan/mvr/core"
	d "github.com/johanan/mvr/data"
	"github.com/spf13/cobra"
)

var mvCfgFile string

var mvCmd = &cobra.Command{
	Use:   "mv",
	Short: "mv is what mvs the data",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		templateData, err := os.ReadFile(mvCfgFile)
		if err != nil {
			log.Fatalf("Error reading template file: %v", err)
		}

		tmpl, err := template.New("base").Funcs(sprig.TxtFuncMap()).Parse(string(templateData))
		if err != nil {
			log.Fatalf("Error parsing template: %v", err)
		}

		tmplVars := d.GetMVRVars("TMPL")

		var renderedConfig bytes.Buffer
		tmpl.Execute(&renderedConfig, tmplVars)

		sConfig, err := d.NewStreamConfigFromYaml(renderedConfig.Bytes())
		if err != nil {
			return fmt.Errorf("error parsing config: %v", err)
		}

		task, err := core.SetupMv(sConfig)
		if err != nil {
			return fmt.Errorf("error setting up task: %v", err)
		}
		err = core.RunMv(ctx, task)
		if err != nil {
			return fmt.Errorf("error running task: %v", err)
		}
		return nil
	},
}

func init() {
	mvCmd.PersistentFlags().StringVarP(&mvCfgFile, "config", "f", "", "config file")
	mvCmd.MarkPersistentFlagRequired("config")
}
