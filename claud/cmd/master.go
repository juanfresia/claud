package cmd

import (
	"fmt"
	linuxproc "github.com/c9s/goprocinfo/linux"
	"github.com/juanfresia/claud/master"
	"github.com/spf13/cobra"
)

var port string
var masterIp string
var masterMem uint64

func init() {
	rootCmd.AddCommand(masterCmd)
	masterCmd.Flags().StringVarP(&masterIp, "ip", "i", "localhost", "IP to run the master HTTP server")
	masterCmd.Flags().StringVarP(&port, "port", "p", "8081", "Port to run the master HTTP server")
	masterCmd.Flags().Uint64VarP(&masterMem, "mem", "m", 1024, "Memory size (in KiB) yielded to claud for running processes")
}

var masterCmd = &cobra.Command{
	Use:   "master",
	Short: "Launches claud master process",
	Run:   masterProcess,
}

func validateMem() bool {
	meminfo, err := linuxproc.ReadMemInfo("/proc/meminfo")
	if err != nil {
		fmt.Println("Couldn't read memory info: " + err.Error())
	}
	meminfo.MemTotal /= 1024 // From B to KiB
	return (meminfo.MemTotal >= masterMem)
}

func masterProcess(cmd *cobra.Command, args []string) {
	fmt.Println("Launching master process...")
	if !validateMem() {
		fmt.Printf("FATAL: %v KiB of memory are not available!\n", masterMem)
		return
	}
	master.LaunchMaster(masterIp, port, masterMem)
}
