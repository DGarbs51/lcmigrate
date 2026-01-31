package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
)

var (
	bold   = color.New(color.Bold).SprintFunc()
	green  = color.New(color.FgGreen).SprintFunc()
	red    = color.New(color.FgRed).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
	cyan   = color.New(color.FgCyan).SprintFunc()
	dim    = color.New(color.Faint).SprintFunc()
)

// Header prints a bold section header
func Header(text string) {
	fmt.Printf("\n  %s\n", bold(text))
	fmt.Printf("  %s\n\n", dim(strings.Repeat("─", len(text)+2)))
}

// SubHeader prints a smaller section header
func SubHeader(text string) {
	fmt.Printf("\n  %s\n", bold(text))
}

// Success prints a green checkmark with message
func Success(text string) {
	fmt.Printf("  %s %s\n", green("✓"), text)
}

// Error prints a red X with message
func Error(text string) {
	fmt.Printf("  %s %s\n", red("✗"), text)
}

// Warning prints a yellow warning with message
func Warning(text string) {
	fmt.Printf("  %s %s\n", yellow("⚠"), text)
}

// Info prints an info message
func Info(text string) {
	fmt.Printf("  %s\n", text)
}

// DryRun prints a dry run prefixed message
func DryRun(text string) {
	fmt.Printf("  %s %s\n", cyan("[DRY RUN]"), text)
}

// Phase prints a migration phase indicator
// Example: [1/6] Migrating schema...
func Phase(current, total int, description string) {
	fmt.Printf("\n  %s %s", cyan(fmt.Sprintf("[%d/%d]", current, total)), description)
}

// PhaseDone completes a phase with timing
// Example: done (2.3s)
func PhaseDone(duration time.Duration) {
	fmt.Printf("  %s (%s)\n", green("done"), formatDuration(duration))
}

// PhaseSkipped marks a phase as skipped
func PhaseSkipped(reason string) {
	fmt.Printf("  %s (%s)\n", dim("skipped"), reason)
}

// PhaseFailed marks a phase as failed
func PhaseFailed(err error) {
	fmt.Printf("  %s\n", red("failed"))
	fmt.Printf("    %s %s\n", red("Error:"), err)
}

// TableProgress prints progress for a table during data transfer
func TableProgress(tableName string, rows int64, total int64) {
	pct := float64(rows) / float64(total) * 100
	fmt.Printf("\r    %s: %s / %s (%.1f%%)", tableName, FormatNumber(rows), FormatNumber(total), pct)
}

// TableDone completes table progress
func TableDone(tableName string, rows int64, duration time.Duration) {
	fmt.Printf("\r    %s %s: %s rows (%s)\n", green("✓"), tableName, FormatNumber(rows), formatDuration(duration))
}

// Confirm prints a confirmation prompt and returns true if user confirms
func Confirm(message string) bool {
	fmt.Printf("  %s (y/n): ", message)
	var response string
	fmt.Scanln(&response)
	return strings.ToLower(strings.TrimSpace(response)) == "y"
}

// Summary prints a final migration summary
func Summary(tables int, rows int64, duration time.Duration) {
	fmt.Println()
	fmt.Printf("  %s\n", green("Migration complete!"))
	fmt.Printf("    Tables: %d\n", tables)
	fmt.Printf("    Rows:   %s\n", FormatNumber(rows))
	fmt.Printf("    Time:   %s\n", formatDuration(duration))
}

// ConnectionInfo prints database connection info
func ConnectionInfo(label, engine, host, port, database string) {
	fmt.Printf("  %s: %s://%s@%s:%s/%s\n", bold(label), engine, "user", host, port, database)
}

// FormatBytes converts bytes to human-readable format
func FormatBytes(bytes float64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%.0f B", bytes)
	}
	div, exp := float64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", bytes/div, "KMGTPE"[exp])
}

// FormatNumber adds comma separators to large numbers
func FormatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// formatDuration converts duration to human-readable format
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, mins)
}
