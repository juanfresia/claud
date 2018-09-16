package cmd

import (
	"fmt"
	"github.com/juanfresia/claud/master"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(masterCmd)
}

var masterCmd = &cobra.Command{
	Use:   "master",
	Short: "Launches claud master process",
	Run:   masterProcess,
}

func masterProcess(cmd *cobra.Command, args []string) {
	fmt.Println("Launching master process")
	master.LaunchMaster()
}
