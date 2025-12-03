package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "postgremq",
	Short: "PostgreMQ database schema management",
	Long:  "CLI tool for managing PostgreMQ database schema and migrations",
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
