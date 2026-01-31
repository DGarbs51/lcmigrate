package cmd

import (
	"fmt"
	"os"

	"github.com/DGarbs51/lcmigrate/db"
	"github.com/DGarbs51/lcmigrate/internal/migrator"
	"github.com/spf13/cobra"
)

var dryRun bool

var rootCmd = &cobra.Command{
	Use:   "lcmigrate",
	Short: "Database analysis and migration tool",
	Long:  `A CLI tool to connect to MySQL or PostgreSQL databases, retrieve detailed analytics, and migrate data between servers.`,
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

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate a database from source to destination",
	Long: `Migrate a MySQL or PostgreSQL database from one server to another.

This command will:
  1. Prompt for source and destination database credentials
  2. Run pre-flight validation checks
  3. Migrate schema (tables, indexes, constraints)
  4. Transfer data in batches
  5. Migrate views and sequences
  6. Verify the migration

Use --dry-run to see what would be migrated without making changes.`,
	Run: func(cmd *cobra.Command, args []string) {
		runMigrate(dryRun)
	},
}

func runMigrate(dryRun bool) {
	if err := migrator.Run(dryRun); err != nil {
		fmt.Printf("Migration failed: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
	rootCmd.AddCommand(migrateCmd)

	// Add flags to migrate command
	migrateCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be migrated without making changes")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
