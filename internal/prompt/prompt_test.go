package prompt

import (
	"bytes"
	goio "io"
	"os"
	"strings"
	"testing"

	"github.com/DGarbs51/lcmigrate/internal/io"
)

// captureStdout captures stdout during function execution
func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	goio.Copy(&buf, r)
	return buf.String()
}

// TestPrompter tests using the mock console

func TestNewPrompter(t *testing.T) {
	mock := io.NewMockConsole(nil, "")
	p := NewPrompter(mock)
	if p == nil {
		t.Errorf("NewPrompter() = nil, want non-nil")
	}
	if p.console == nil {
		t.Errorf("NewPrompter().console = nil, want non-nil")
	}
}

func TestDefaultPrompter(t *testing.T) {
	p := DefaultPrompter()
	if p == nil {
		t.Errorf("DefaultPrompter() = nil, want non-nil")
	}
	if p.console == nil {
		t.Errorf("DefaultPrompter().console = nil, want non-nil")
	}
}

func TestPrompter_PromptWithDefault_UseInput(t *testing.T) {
	mock := io.NewMockConsole([]string{"custom_value"}, "")
	p := NewPrompter(mock)

	result := p.PromptWithDefault("Test prompt", "default")

	if result != "custom_value" {
		t.Errorf("PromptWithDefault() = %q, want %q", result, "custom_value")
	}
}

func TestPrompter_PromptWithDefault_UseDefault(t *testing.T) {
	mock := io.NewMockConsole([]string{""}, "")
	p := NewPrompter(mock)

	result := p.PromptWithDefault("Test prompt", "default_value")

	if result != "default_value" {
		t.Errorf("PromptWithDefault() = %q, want %q", result, "default_value")
	}
}

func TestPrompter_PromptWithDefault_NoDefault(t *testing.T) {
	mock := io.NewMockConsole([]string{"user_input"}, "")
	p := NewPrompter(mock)

	result := p.PromptWithDefault("Test prompt", "")

	if result != "user_input" {
		t.Errorf("PromptWithDefault() = %q, want %q", result, "user_input")
	}

	// Verify prompt format doesn't include brackets for empty default
	output := mock.GetOutput()
	if strings.Contains(output, "[") {
		t.Errorf("PromptWithDefault() with empty default should not show brackets, got %q", output)
	}
}

func TestPrompter_PromptWithDefault_EmptyInputNoDefault(t *testing.T) {
	mock := io.NewMockConsole([]string{""}, "")
	p := NewPrompter(mock)

	result := p.PromptWithDefault("Test prompt", "")

	if result != "" {
		t.Errorf("PromptWithDefault() = %q, want empty string", result)
	}
}

func TestPrompter_ReadPassword_UsePassword(t *testing.T) {
	mock := io.NewMockConsole(nil, "secret123")
	p := NewPrompter(mock)

	result := p.ReadPassword("Password", "")

	if result != "secret123" {
		t.Errorf("ReadPassword() = %q, want %q", result, "secret123")
	}
}

func TestPrompter_ReadPassword_UseDefault(t *testing.T) {
	mock := io.NewMockConsole(nil, "")
	p := NewPrompter(mock)

	result := p.ReadPassword("Password", "default_pass")

	if result != "default_pass" {
		t.Errorf("ReadPassword() = %q, want %q", result, "default_pass")
	}
}

func TestPrompter_ReadPassword_WithDefaultShown(t *testing.T) {
	mock := io.NewMockConsole(nil, "newpass")
	p := NewPrompter(mock)

	_ = p.ReadPassword("Password", "old_pass")

	output := mock.GetOutput()
	if !strings.Contains(output, "[****]") {
		t.Errorf("ReadPassword() with default should show [****], got %q", output)
	}
}

func TestPrompter_Confirm_Yes(t *testing.T) {
	mock := io.NewMockConsole([]string{"y"}, "")
	p := NewPrompter(mock)

	result := p.Confirm("Continue?")

	if !result {
		t.Errorf("Confirm('y') = false, want true")
	}
}

func TestPrompter_Confirm_YesFull(t *testing.T) {
	mock := io.NewMockConsole([]string{"yes"}, "")
	p := NewPrompter(mock)

	result := p.Confirm("Continue?")

	if !result {
		t.Errorf("Confirm('yes') = false, want true")
	}
}

func TestPrompter_Confirm_YesUpperCase(t *testing.T) {
	mock := io.NewMockConsole([]string{"YES"}, "")
	p := NewPrompter(mock)

	result := p.Confirm("Continue?")

	if !result {
		t.Errorf("Confirm('YES') = false, want true")
	}
}

func TestPrompter_Confirm_No(t *testing.T) {
	mock := io.NewMockConsole([]string{"n"}, "")
	p := NewPrompter(mock)

	result := p.Confirm("Continue?")

	if result {
		t.Errorf("Confirm('n') = true, want false")
	}
}

func TestPrompter_Confirm_Invalid(t *testing.T) {
	mock := io.NewMockConsole([]string{"maybe"}, "")
	p := NewPrompter(mock)

	result := p.Confirm("Continue?")

	if result {
		t.Errorf("Confirm('maybe') = true, want false")
	}
}

func TestPrompter_ConfirmWithWarning(t *testing.T) {
	mock := io.NewMockConsole([]string{"y"}, "")
	p := NewPrompter(mock)

	captureStdout(func() {
		result := p.ConfirmWithWarning("This is dangerous!", "Proceed?")
		if !result {
			t.Errorf("ConfirmWithWarning() = false, want true")
		}
	})
}

func TestPrompter_PromptSourceDatabase(t *testing.T) {
	// Provide inputs for: engine, host, port, database, user, password
	inputs := []string{"mysql", "localhost", "3306", "testdb", "testuser"}
	mock := io.NewMockConsole(inputs, "testpass")
	p := NewPrompter(mock)

	// Clear env vars to avoid interference
	envVars := []string{
		"SOURCE_DB_ENGINE", "DB_ENGINE", "SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_PORT", "DB_PORT", "SOURCE_DB_DATABASE", "DB_DATABASE",
		"SOURCE_DB_USER", "DB_USER", "SOURCE_DB_PASSWORD", "DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	captureStdout(func() {
		cfg := p.PromptSourceDatabase()

		if cfg.Engine != "mysql" {
			t.Errorf("Engine = %q, want %q", cfg.Engine, "mysql")
		}
		if cfg.Host != "localhost" {
			t.Errorf("Host = %q, want %q", cfg.Host, "localhost")
		}
		if cfg.Port != "3306" {
			t.Errorf("Port = %q, want %q", cfg.Port, "3306")
		}
		if cfg.Database != "testdb" {
			t.Errorf("Database = %q, want %q", cfg.Database, "testdb")
		}
		if cfg.User != "testuser" {
			t.Errorf("User = %q, want %q", cfg.User, "testuser")
		}
		if cfg.Password != "testpass" {
			t.Errorf("Password = %q, want %q", cfg.Password, "testpass")
		}
	})
}

func TestPrompter_PromptSourceDatabase_WithDefaults(t *testing.T) {
	// Empty inputs should use defaults
	inputs := []string{"", "", "", "mydb", "", ""}
	mock := io.NewMockConsole(inputs, "")
	p := NewPrompter(mock)

	// Clear env vars
	envVars := []string{
		"SOURCE_DB_ENGINE", "DB_ENGINE", "SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_PORT", "DB_PORT", "SOURCE_DB_DATABASE", "DB_DATABASE",
		"SOURCE_DB_USER", "DB_USER", "SOURCE_DB_PASSWORD", "DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	captureStdout(func() {
		cfg := p.PromptSourceDatabase()

		// Default engine is mysql
		if cfg.Engine != "mysql" {
			t.Errorf("Engine = %q, want %q", cfg.Engine, "mysql")
		}
		// Default host is localhost
		if cfg.Host != "localhost" {
			t.Errorf("Host = %q, want %q", cfg.Host, "localhost")
		}
		// Default port for mysql is 3306
		if cfg.Port != "3306" {
			t.Errorf("Port = %q, want %q", cfg.Port, "3306")
		}
		// Database was provided
		if cfg.Database != "mydb" {
			t.Errorf("Database = %q, want %q", cfg.Database, "mydb")
		}
		// Default user is root
		if cfg.User != "root" {
			t.Errorf("User = %q, want %q", cfg.User, "root")
		}
	})
}

func TestPrompter_PromptSourceDatabase_PostgreSQL(t *testing.T) {
	inputs := []string{"pgsql", "", "", "pgdb", "", ""}
	mock := io.NewMockConsole(inputs, "pgpass")
	p := NewPrompter(mock)

	// Clear env vars
	envVars := []string{
		"SOURCE_DB_ENGINE", "DB_ENGINE", "SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_PORT", "DB_PORT", "SOURCE_DB_DATABASE", "DB_DATABASE",
		"SOURCE_DB_USER", "DB_USER", "SOURCE_DB_PASSWORD", "DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	captureStdout(func() {
		cfg := p.PromptSourceDatabase()

		if cfg.Engine != "pgsql" {
			t.Errorf("Engine = %q, want %q", cfg.Engine, "pgsql")
		}
		// Default port for pgsql is 5432
		if cfg.Port != "5432" {
			t.Errorf("Port = %q, want %q", cfg.Port, "5432")
		}
	})
}

func TestPrompter_PromptDestinationDatabase(t *testing.T) {
	// Provide inputs for: host, port, database, user, password
	inputs := []string{"dest-host", "5433", "destdb", "destuser"}
	mock := io.NewMockConsole(inputs, "destpass")
	p := NewPrompter(mock)

	// Clear env vars
	envVars := []string{
		"DESTINATION_DB_HOST", "DESTINATION_DB_PORT",
		"DESTINATION_DB_DATABASE", "DESTINATION_DB_USER", "DESTINATION_DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	captureStdout(func() {
		cfg := p.PromptDestinationDatabase("pgsql")

		// Engine should be locked to source engine
		if cfg.Engine != "pgsql" {
			t.Errorf("Engine = %q, want %q", cfg.Engine, "pgsql")
		}
		if cfg.Host != "dest-host" {
			t.Errorf("Host = %q, want %q", cfg.Host, "dest-host")
		}
		if cfg.Port != "5433" {
			t.Errorf("Port = %q, want %q", cfg.Port, "5433")
		}
		if cfg.Database != "destdb" {
			t.Errorf("Database = %q, want %q", cfg.Database, "destdb")
		}
		if cfg.User != "destuser" {
			t.Errorf("User = %q, want %q", cfg.User, "destuser")
		}
		if cfg.Password != "destpass" {
			t.Errorf("Password = %q, want %q", cfg.Password, "destpass")
		}
	})
}

func TestPrompter_PromptMigrationConfig(t *testing.T) {
	// Source inputs (5) + destination inputs (4)
	inputs := []string{
		"mysql", "src-host", "3306", "srcdb", "srcuser",
		"dest-host", "3307", "destdb", "destuser",
	}
	mock := io.NewMockConsole(inputs, "password")
	p := NewPrompter(mock)

	// Clear all env vars
	envVars := []string{
		"SOURCE_DB_ENGINE", "DB_ENGINE", "SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_PORT", "DB_PORT", "SOURCE_DB_DATABASE", "DB_DATABASE",
		"SOURCE_DB_USER", "DB_USER", "SOURCE_DB_PASSWORD", "DB_PASSWORD",
		"DESTINATION_DB_HOST", "DESTINATION_DB_PORT",
		"DESTINATION_DB_DATABASE", "DESTINATION_DB_USER", "DESTINATION_DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	captureStdout(func() {
		cfg := p.PromptMigrationConfig(false)

		if cfg.Source.Engine != "mysql" {
			t.Errorf("Source.Engine = %q, want %q", cfg.Source.Engine, "mysql")
		}
		if cfg.Destination.Engine != "mysql" {
			t.Errorf("Destination.Engine = %q, want %q", cfg.Destination.Engine, "mysql")
		}
		if cfg.DryRun {
			t.Errorf("DryRun = true, want false")
		}
	})
}

func TestPrompter_PromptMigrationConfig_DryRun(t *testing.T) {
	inputs := []string{
		"mysql", "", "", "db", "",
		"", "", "destdb", "",
	}
	mock := io.NewMockConsole(inputs, "")
	p := NewPrompter(mock)

	// Clear all env vars
	envVars := []string{
		"SOURCE_DB_ENGINE", "DB_ENGINE", "SOURCE_DB_HOST", "DB_HOST",
		"SOURCE_DB_PORT", "DB_PORT", "SOURCE_DB_DATABASE", "DB_DATABASE",
		"SOURCE_DB_USER", "DB_USER", "SOURCE_DB_PASSWORD", "DB_PASSWORD",
		"DESTINATION_DB_HOST", "DESTINATION_DB_PORT",
		"DESTINATION_DB_DATABASE", "DESTINATION_DB_USER", "DESTINATION_DB_PASSWORD",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	captureStdout(func() {
		cfg := p.PromptMigrationConfig(true)

		if !cfg.DryRun {
			t.Errorf("DryRun = false, want true")
		}

		// Verify dry run mode was indicated in output
		output := mock.GetOutput()
		if !strings.Contains(output, "DRY RUN") {
			t.Errorf("DryRun mode should show [DRY RUN], got %q", output)
		}
	})
}

// Tests for legacy functions (backward compatibility)

func TestConfirm_Yes(t *testing.T) {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("y\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if !result {
			t.Errorf("Confirm() with 'y' = false, want true")
		}
	})

	os.Stdin = oldStdin
}

func TestConfirm_YesFullWord(t *testing.T) {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("yes\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if !result {
			t.Errorf("Confirm() with 'yes' = false, want true")
		}
	})

	os.Stdin = oldStdin
}

func TestConfirm_No(t *testing.T) {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("n\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if result {
			t.Errorf("Confirm() with 'n' = true, want false")
		}
	})

	os.Stdin = oldStdin
}

func TestConfirm_Invalid(t *testing.T) {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("maybe\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if result {
			t.Errorf("Confirm() with 'maybe' = true, want false")
		}
	})

	os.Stdin = oldStdin
}

func TestConfirm_CaseInsensitive(t *testing.T) {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("YES\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if !result {
			t.Errorf("Confirm() with 'YES' = false, want true")
		}
	})

	os.Stdin = oldStdin
}

func TestConfirmWithWarning(t *testing.T) {
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r

	go func() {
		w.Write([]byte("y\n"))
		w.Close()
	}()

	output := captureStdout(func() {
		result := ConfirmWithWarning("This is dangerous!", "Proceed?")
		if !result {
			t.Errorf("ConfirmWithWarning() with 'y' = false, want true")
		}
	})

	os.Stdin = oldStdin

	if !strings.Contains(output, "This is dangerous!") {
		t.Errorf("ConfirmWithWarning() output should contain warning message")
	}
}

// ErrorMockConsole is a mock that returns errors
type ErrorMockConsole struct {
	io.MockConsole
}

func (e *ErrorMockConsole) ReadPassword() (string, error) {
	return "", goio.EOF
}

func TestPrompter_ReadPassword_Error(t *testing.T) {
	mock := &ErrorMockConsole{
		MockConsole: *io.NewMockConsole([]string{"fallback_input"}, ""),
	}
	p := &Prompter{console: mock}

	result := p.ReadPassword("Password", "default")

	// Should fall back to ReadLine
	if result != "fallback_input" {
		t.Errorf("ReadPassword() with error = %q, want %q", result, "fallback_input")
	}
}

func TestPrompter_ReadPassword_ErrorWithDefault(t *testing.T) {
	mock := &ErrorMockConsole{
		MockConsole: *io.NewMockConsole([]string{""}, ""),
	}
	p := &Prompter{console: mock}

	result := p.ReadPassword("Password", "default_value")

	// Should use default when fallback input is empty
	if result != "default_value" {
		t.Errorf("ReadPassword() with error and empty input = %q, want %q", result, "default_value")
	}
}
