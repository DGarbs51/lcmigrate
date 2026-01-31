package cmd

import (
	"fmt"
	"os"

	"github.com/DGarbs51/lcmigrate/db"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "lcmigrate",
	Short: "Database analysis and migration tool",
	Long:  `A CLI tool to connect to MySQL or PostgreSQL databases and retrieve detailed analytics.`,
}

var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Analyze a database and display statistics",
	Long:  `Connect to a MySQL or PostgreSQL database and display comprehensive statistics including table counts, sizes, schema details, and more.`,
	Run: func(cmd *cobra.Command, args []string) {
		config := db.PromptConnectionDetails()

		conn, err := db.Connect(config)
		if err != nil {
			fmt.Printf("Error connecting to database: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close()

		fmt.Println("\n=== Database Analysis ===")

		if err := db.Analyze(conn, config); err != nil {
			fmt.Printf("Error analyzing database: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
