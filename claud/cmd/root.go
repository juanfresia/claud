package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "claud",
	Short: "CLAUD: Cluster for Sistemas Distribuidos II, FIUBA.",
	Long:  "CLAUD: Cluster for Sistemas Distribuidos II, FIUBA.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Hello world!")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
