package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "mvr",
	Short: "mvr is a tool for moving data",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	viper.AutomaticEnv()
	viper.SetEnvPrefix("mvr")
	rootCmd.AddCommand(mvCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
