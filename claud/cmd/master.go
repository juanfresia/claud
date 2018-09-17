package cmd

import (
	"fmt"
	"github.com/juanfresia/claud/master"
	"github.com/spf13/cobra"
)

var port string
var masterIp string

func init() {
	rootCmd.AddCommand(masterCmd)
	masterCmd.Flags().StringVarP(&masterIp, "ip", "i", "localhost", "IP to run the master HTTP server")
	masterCmd.Flags().StringVarP(&port, "port", "p", "8081", "Port to run the master HTTP server")
}

var masterCmd = &cobra.Command{
	Use:   "master",
	Short: "Launches claud master process",
	Run:   masterProcess,
}

func masterProcess(cmd *cobra.Command, args []string) {
	fmt.Println("Launching master process...")
	master.LaunchMaster(masterIp, port)
}
