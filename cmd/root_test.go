package cmd

import (
	"testing"
)

func TestRootCmd(t *testing.T) {
	// Test that rootCmd is initialized
	if rootCmd == nil {
		t.Errorf("rootCmd = nil, want non-nil")
	}

	// Test command properties
	if rootCmd.Use != "lcmigrate" {
		t.Errorf("rootCmd.Use = %q, want %q", rootCmd.Use, "lcmigrate")
	}

	if rootCmd.Short == "" {
		t.Errorf("rootCmd.Short should not be empty")
	}

	if rootCmd.Long == "" {
		t.Errorf("rootCmd.Long should not be empty")
	}
}

func TestAnalyzeCmd(t *testing.T) {
	// Test that analyzeCmd is initialized
	if analyzeCmd == nil {
		t.Errorf("analyzeCmd = nil, want non-nil")
	}

	// Test command properties
	if analyzeCmd.Use != "analyze" {
		t.Errorf("analyzeCmd.Use = %q, want %q", analyzeCmd.Use, "analyze")
	}

	if analyzeCmd.Short == "" {
		t.Errorf("analyzeCmd.Short should not be empty")
	}

	if analyzeCmd.Run == nil {
		t.Errorf("analyzeCmd.Run = nil, want non-nil")
	}
}

func TestMigrateCmd(t *testing.T) {
	// Test that migrateCmd is initialized
	if migrateCmd == nil {
		t.Errorf("migrateCmd = nil, want non-nil")
	}

	// Test command properties
	if migrateCmd.Use != "migrate" {
		t.Errorf("migrateCmd.Use = %q, want %q", migrateCmd.Use, "migrate")
	}

	if migrateCmd.Short == "" {
		t.Errorf("migrateCmd.Short should not be empty")
	}

	if migrateCmd.Run == nil {
		t.Errorf("migrateCmd.Run = nil, want non-nil")
	}

	// Test dry-run flag exists
	flag := migrateCmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Errorf("migrateCmd should have --dry-run flag")
	}
	if flag != nil && flag.DefValue != "false" {
		t.Errorf("--dry-run default = %q, want %q", flag.DefValue, "false")
	}
}

func TestInit(t *testing.T) {
	// Verify subcommands are registered
	subcommands := rootCmd.Commands()

	hasAnalyze := false
	hasMigrate := false

	for _, cmd := range subcommands {
		if cmd.Use == "analyze" {
			hasAnalyze = true
		}
		if cmd.Use == "migrate" {
			hasMigrate = true
		}
	}

	if !hasAnalyze {
		t.Errorf("analyze command not registered")
	}
	if !hasMigrate {
		t.Errorf("migrate command not registered")
	}
}

func TestDryRunFlag(t *testing.T) {
	// Initial value should be false (from default)
	flag := migrateCmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatalf("--dry-run flag not found")
	}

	// The variable should be accessible
	// Note: We can't directly test the dryRun variable without executing the command
	// This test verifies the flag is properly configured
	if flag.Name != "dry-run" {
		t.Errorf("flag.Name = %q, want %q", flag.Name, "dry-run")
	}
}
