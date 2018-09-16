package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(loopCmd)
}

var loopCmd = &cobra.Command{
	Use:   "loop",
	Short: "Loop command example just for now",
	Run:   loopExample,
}

func loopExample(cmd *cobra.Command, args []string) {
	for i := 0; i < 10; i++ {
		fmt.Println(i)
	}
}
