package prompt

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/DGarbs51/lcmigrate/internal/config"
	"github.com/DGarbs51/lcmigrate/internal/ui"
	"github.com/fatih/color"
	"golang.org/x/term"
)

var (
	bold = color.New(color.Bold).SprintFunc()
	cyan = color.New(color.FgCyan).SprintFunc()
	dim  = color.New(color.Faint).SprintFunc()
	green = color.New(color.FgGreen).SprintFunc()
)

// promptWithDefault prompts the user for input with an optional default value
func promptWithDefault(reader *bufio.Reader, prompt, defaultVal string) string {
	if defaultVal != "" {
		fmt.Printf("  %s %s: ", cyan(prompt), dim("["+defaultVal+"]"))
	} else {
		fmt.Printf("  %s: ", cyan(prompt))
	}

	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input == "" && defaultVal != "" {
		return defaultVal
	}
	return input
}

// readPassword reads a password from the terminal with masked input
func readPassword(prompt string, defaultVal string) string {
	if defaultVal != "" {
		fmt.Printf("  %s %s: ", cyan(prompt), dim("[****]"))
	} else {
		fmt.Printf("  %s: ", cyan(prompt))
	}

	// Try to read password with masking
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // Print newline after password input

	if err != nil {
		// Fallback to regular input if terminal password reading fails
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" && defaultVal != "" {
			return defaultVal
		}
		return input
	}

	pwd := strings.TrimSpace(string(password))
	if pwd == "" && defaultVal != "" {
		return defaultVal
	}
	return pwd
}

// PromptSourceDatabase prompts for source database credentials
func PromptSourceDatabase() config.DatabaseConfig {
	reader := bufio.NewReader(os.Stdin)
	defaults := config.LoadSourceDefaults()

	ui.Header("Source Database")

	if config.HasEnvDefaults() {
		fmt.Printf("  %s\n\n", green("✓ Found .env file, using values as defaults"))
	}

	// Engine
	engineDefault := defaults.Engine
	if engineDefault == "" {
		engineDefault = "mysql"
	}
	engine := promptWithDefault(reader, "Database engine (mysql/pgsql)", engineDefault)
	engine = config.NormalizeEngine(engine)

	// Host
	hostDefault := defaults.Host
	if hostDefault == "" {
		hostDefault = "localhost"
	}
	host := promptWithDefault(reader, "Host", hostDefault)

	// Port
	portDefault := defaults.Port
	if portDefault == "" {
		portDefault = config.DefaultPort(engine)
	}
	port := promptWithDefault(reader, "Port", portDefault)

	// Database
	database := promptWithDefault(reader, "Database name", defaults.Database)

	// User
	userDefault := defaults.User
	if userDefault == "" {
		userDefault = "root"
	}
	user := promptWithDefault(reader, "User", userDefault)

	// Password
	password := readPassword("Password", defaults.Password)

	return config.DatabaseConfig{
		Engine:   engine,
		Host:     host,
		Port:     port,
		Database: database,
		User:     user,
		Password: password,
	}
}

// PromptDestinationDatabase prompts for destination database credentials
// The engine is locked to match the source database
func PromptDestinationDatabase(sourceEngine string) config.DatabaseConfig {
	reader := bufio.NewReader(os.Stdin)
	defaults := config.LoadDestinationDefaults()

	ui.Header("Destination Database")

	// Engine is locked to match source
	fmt.Printf("  %s %s\n", cyan("Database engine:"), bold(sourceEngine)+" (must match source)")

	// Host
	hostDefault := defaults.Host
	if hostDefault == "" {
		hostDefault = "localhost"
	}
	host := promptWithDefault(reader, "Host", hostDefault)

	// Port
	portDefault := defaults.Port
	if portDefault == "" {
		portDefault = config.DefaultPort(sourceEngine)
	}
	port := promptWithDefault(reader, "Port", portDefault)

	// Database
	database := promptWithDefault(reader, "Database name", defaults.Database)

	// User
	userDefault := defaults.User
	if userDefault == "" {
		userDefault = "root"
	}
	user := promptWithDefault(reader, "User", userDefault)

	// Password
	password := readPassword("Password", defaults.Password)

	return config.DatabaseConfig{
		Engine:   sourceEngine,
		Host:     host,
		Port:     port,
		Database: database,
		User:     user,
		Password: password,
	}
}

// PromptMigrationConfig prompts for both source and destination databases
func PromptMigrationConfig(dryRun bool) config.MigrationConfig {
	config.LoadEnv()

	fmt.Println()
	fmt.Printf("  %s\n", bold("lcmigrate - Database Migration Tool"))
	if dryRun {
		fmt.Printf("  %s\n", cyan("[DRY RUN MODE]"))
	}

	source := PromptSourceDatabase()
	destination := PromptDestinationDatabase(source.Engine)

	return config.MigrationConfig{
		Source:      source,
		Destination: destination,
		DryRun:      dryRun,
	}
}

// Confirm asks the user for a yes/no confirmation
func Confirm(message string) bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("  %s (y/n): ", message)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(strings.ToLower(input))
	return input == "y" || input == "yes"
}

// ConfirmWithWarning asks for confirmation with a warning message
func ConfirmWithWarning(warning string, message string) bool {
	fmt.Println()
	ui.Warning(warning)
	return Confirm(message)
}
