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
	Run: func(cmd *cobra.Command, args []string) {
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
		// output the rendered config to stdout
		fmt.Println(renderedConfig.String())

		sConfig, err := d.NewStreamConfigFromYaml(renderedConfig.Bytes())
		if err != nil {
			fmt.Println("Error parsing config file:", err)
			os.Exit(1)
		}

		task, err := core.SetupMv(sConfig)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		err = core.RunMv(task)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	mvCmd.PersistentFlags().StringVarP(&mvCfgFile, "config", "f", "", "config file")
	mvCmd.MarkPersistentFlagRequired("config")
}
