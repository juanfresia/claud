package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	version string
	commit  string
	branch  string
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version and build status",
	Run:   printVersion,
}

func printVersion(cmd *cobra.Command, args []string) {
	fmt.Println("claud")
	fmt.Println("    version: ", version)
	fmt.Println("    commit:  ", commit)
	fmt.Println("    branch:  ", branch)
}
