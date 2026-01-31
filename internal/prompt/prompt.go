package prompt

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/DGarbs51/lcmigrate/internal/config"
	"github.com/DGarbs51/lcmigrate/internal/io"
	"github.com/DGarbs51/lcmigrate/internal/ui"
	"github.com/fatih/color"
	"golang.org/x/term"
)

var (
	bold  = color.New(color.Bold).SprintFunc()
	cyan  = color.New(color.FgCyan).SprintFunc()
	dim   = color.New(color.Faint).SprintFunc()
	green = color.New(color.FgGreen).SprintFunc()
)

// Prompter handles user prompts with injectable I/O
type Prompter struct {
	console io.Console
}

// NewPrompter creates a new prompter with the given console
func NewPrompter(console io.Console) *Prompter {
	return &Prompter{console: console}
}

// DefaultPrompter returns a prompter using standard I/O
func DefaultPrompter() *Prompter {
	return &Prompter{console: io.NewStdConsole()}
}

// PromptWithDefault prompts the user for input with an optional default value
func (p *Prompter) PromptWithDefault(prompt, defaultVal string) string {
	if defaultVal != "" {
		p.console.Printf("  %s %s: ", cyan(prompt), dim("["+defaultVal+"]"))
	} else {
		p.console.Printf("  %s: ", cyan(prompt))
	}

	input, _ := p.console.ReadLine()
	input = strings.TrimSpace(input)

	if input == "" && defaultVal != "" {
		return defaultVal
	}
	return input
}

// ReadPassword reads a password with optional masking
func (p *Prompter) ReadPassword(prompt string, defaultVal string) string {
	if defaultVal != "" {
		p.console.Printf("  %s %s: ", cyan(prompt), dim("[****]"))
	} else {
		p.console.Printf("  %s: ", cyan(prompt))
	}

	pwd, err := p.console.ReadPassword()
	if err != nil {
		// Fallback: try reading as regular line
		input, _ := p.console.ReadLine()
		input = strings.TrimSpace(input)
		if input == "" && defaultVal != "" {
			return defaultVal
		}
		return input
	}

	pwd = strings.TrimSpace(pwd)
	if pwd == "" && defaultVal != "" {
		return defaultVal
	}
	return pwd
}

// PromptSourceDatabase prompts for source database credentials
func (p *Prompter) PromptSourceDatabase() config.DatabaseConfig {
	defaults := config.LoadSourceDefaults()

	ui.Header("Source Database")

	if config.HasEnvDefaults() {
		p.console.Printf("  %s\n\n", green("✓ Found .env file, using values as defaults"))
	}

	// Engine
	engineDefault := defaults.Engine
	if engineDefault == "" {
		engineDefault = "mysql"
	}
	engine := p.PromptWithDefault("Database engine (mysql/pgsql)", engineDefault)
	engine = config.NormalizeEngine(engine)

	// Host
	hostDefault := defaults.Host
	if hostDefault == "" {
		hostDefault = "localhost"
	}
	host := p.PromptWithDefault("Host", hostDefault)

	// Port
	portDefault := defaults.Port
	if portDefault == "" {
		portDefault = config.DefaultPort(engine)
	}
	port := p.PromptWithDefault("Port", portDefault)

	// Database
	database := p.PromptWithDefault("Database name", defaults.Database)

	// User
	userDefault := defaults.User
	if userDefault == "" {
		userDefault = "root"
	}
	user := p.PromptWithDefault("User", userDefault)

	// Password
	password := p.ReadPassword("Password", defaults.Password)

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
func (p *Prompter) PromptDestinationDatabase(sourceEngine string) config.DatabaseConfig {
	defaults := config.LoadDestinationDefaults()

	ui.Header("Destination Database")

	// Engine is locked to match source
	p.console.Printf("  %s %s\n", cyan("Database engine:"), bold(sourceEngine)+" (must match source)")

	// Host
	hostDefault := defaults.Host
	if hostDefault == "" {
		hostDefault = "localhost"
	}
	host := p.PromptWithDefault("Host", hostDefault)

	// Port
	portDefault := defaults.Port
	if portDefault == "" {
		portDefault = config.DefaultPort(sourceEngine)
	}
	port := p.PromptWithDefault("Port", portDefault)

	// Database
	database := p.PromptWithDefault("Database name", defaults.Database)

	// User
	userDefault := defaults.User
	if userDefault == "" {
		userDefault = "root"
	}
	user := p.PromptWithDefault("User", userDefault)

	// Password
	password := p.ReadPassword("Password", defaults.Password)

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
func (p *Prompter) PromptMigrationConfig(dryRun bool) config.MigrationConfig {
	config.LoadEnv()

	p.console.Println()
	p.console.Printf("  %s\n", bold("lcmigrate - Database Migration Tool"))
	if dryRun {
		p.console.Printf("  %s\n", cyan("[DRY RUN MODE]"))
	}

	source := p.PromptSourceDatabase()
	destination := p.PromptDestinationDatabase(source.Engine)

	return config.MigrationConfig{
		Source:      source,
		Destination: destination,
		DryRun:      dryRun,
	}
}

// Confirm asks the user for a yes/no confirmation
func (p *Prompter) Confirm(message string) bool {
	p.console.Printf("  %s (y/n): ", message)
	input, _ := p.console.ReadLine()
	input = strings.TrimSpace(strings.ToLower(input))
	return input == "y" || input == "yes"
}

// ConfirmWithWarning asks for confirmation with a warning message
func (p *Prompter) ConfirmWithWarning(warning string, message string) bool {
	p.console.Println()
	ui.Warning(warning)
	return p.Confirm(message)
}

// Legacy functions for backward compatibility - use DefaultPrompter

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
