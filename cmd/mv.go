package cmd

import (
	"fmt"
	"os"

	"github.com/johanan/mvr/core"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var mvCfgFile string

var mvCmd = &cobra.Command{
	Use:   "mv",
	Short: "mv is what mvs the data",
	Run: func(cmd *cobra.Command, args []string) {
		v := viper.New()
		v.SetConfigFile(mvCfgFile)
		v.ReadInConfig()

		var sConfig core.StreamConfig
		err := v.Unmarshal(&sConfig)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		task, err := core.SetupDb2File(&sConfig)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		err = core.RunDb2File(task)
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
