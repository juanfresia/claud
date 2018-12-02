package cmd

import (
	"fmt"
	linuxproc "github.com/c9s/goprocinfo/linux"
	"github.com/juanfresia/claud/slave"
	"github.com/spf13/cobra"
)

var slavePort string
var slaveIp string
var slaveMem uint64

func init() {
	rootCmd.AddCommand(slaveCmd)
	slaveCmd.Flags().StringVarP(&slaveIp, "ip", "i", "localhost", "IP to run the slave HTTP server")
	slaveCmd.Flags().StringVarP(&slavePort, "port", "p", "8081", "Port to run the slave HTTP server")
	slaveCmd.Flags().Uint64VarP(&slaveMem, "memory", "m", 1024, "Memory size (in KiB) yielded to claud for running processes")
}

var slaveCmd = &cobra.Command{
	Use:   "slave",
	Short: "Launches claud slave process",
	Run:   slaveProcess,
}

func validateSlaveMem() bool {
	meminfo, err := linuxproc.ReadMemInfo("/proc/meminfo")
	if err != nil {
		fmt.Println("Couldn't read memory info: " + err.Error())
	}
	meminfo.MemTotal /= 1024 // From B to KiB
	return (meminfo.MemTotal >= slaveMem)
}

func slaveProcess(cmd *cobra.Command, args []string) {
	fmt.Println("Launching slave process...")
	if !validateSlaveMem() {
		fmt.Printf("FATAL: %v KiB of memory are not available!\n", slaveMem)
		return
	}
	slave.LaunchSlave(slaveIp, slavePort, slaveMem, 2)
}
