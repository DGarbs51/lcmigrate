package ui

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes float64
		want  string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
		{1125899906842624, "1.0 PB"},
		{2.5 * 1024 * 1024, "2.5 MB"},
	}

	for _, tt := range tests {
		got := FormatBytes(tt.bytes)
		if got != tt.want {
			t.Errorf("FormatBytes(%v) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{12, "12"},
		{123, "123"},
		{999, "999"},
		{1000, "1,000"},
		{1234, "1,234"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1000000, "1,000,000"},
		{1234567890, "1,234,567,890"},
		{-1, "-1"},
		{-1234, "-1,234"},
	}

	for _, tt := range tests {
		got := FormatNumber(tt.n)
		if got != tt.want {
			t.Errorf("FormatNumber(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "0ms"},
		{500 * time.Millisecond, "500ms"},
		{999 * time.Millisecond, "999ms"},
		{1 * time.Second, "1.0s"},
		{1500 * time.Millisecond, "1.5s"},
		{30 * time.Second, "30.0s"},
		{59 * time.Second, "59.0s"},
		{60 * time.Second, "1m 0s"},
		{90 * time.Second, "1m 30s"},
		{5*time.Minute + 30*time.Second, "5m 30s"},
		{59*time.Minute + 59*time.Second, "59m 59s"},
		{60 * time.Minute, "1h 0m"},
		{90 * time.Minute, "1h 30m"},
		{2*time.Hour + 45*time.Minute, "2h 45m"},
	}

	for _, tt := range tests {
		got := formatDuration(tt.d)
		if got != tt.want {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

// captureStdout captures stdout during function execution
func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

func TestHeader(t *testing.T) {
	output := captureStdout(func() {
		Header("Test Header")
	})

	if !strings.Contains(output, "Test Header") {
		t.Errorf("Header() output should contain 'Test Header', got %q", output)
	}
	if !strings.Contains(output, "─") {
		t.Errorf("Header() output should contain separator line, got %q", output)
	}
}

func TestSubHeader(t *testing.T) {
	output := captureStdout(func() {
		SubHeader("Test SubHeader")
	})

	if !strings.Contains(output, "Test SubHeader") {
		t.Errorf("SubHeader() output should contain 'Test SubHeader', got %q", output)
	}
}

func TestSuccess(t *testing.T) {
	output := captureStdout(func() {
		Success("Operation successful")
	})

	if !strings.Contains(output, "Operation successful") {
		t.Errorf("Success() output should contain message, got %q", output)
	}
	if !strings.Contains(output, "✓") {
		t.Errorf("Success() output should contain checkmark, got %q", output)
	}
}

func TestError(t *testing.T) {
	output := captureStdout(func() {
		Error("Operation failed")
	})

	if !strings.Contains(output, "Operation failed") {
		t.Errorf("Error() output should contain message, got %q", output)
	}
	if !strings.Contains(output, "✗") {
		t.Errorf("Error() output should contain X mark, got %q", output)
	}
}

func TestWarning(t *testing.T) {
	output := captureStdout(func() {
		Warning("Warning message")
	})

	if !strings.Contains(output, "Warning message") {
		t.Errorf("Warning() output should contain message, got %q", output)
	}
	if !strings.Contains(output, "⚠") {
		t.Errorf("Warning() output should contain warning symbol, got %q", output)
	}
}

func TestInfo(t *testing.T) {
	output := captureStdout(func() {
		Info("Info message")
	})

	if !strings.Contains(output, "Info message") {
		t.Errorf("Info() output should contain message, got %q", output)
	}
}

func TestDryRun(t *testing.T) {
	output := captureStdout(func() {
		DryRun("Would do something")
	})

	if !strings.Contains(output, "[DRY RUN]") {
		t.Errorf("DryRun() output should contain '[DRY RUN]', got %q", output)
	}
	if !strings.Contains(output, "Would do something") {
		t.Errorf("DryRun() output should contain message, got %q", output)
	}
}

func TestPhase(t *testing.T) {
	output := captureStdout(func() {
		Phase(1, 6, "Migrating schema...")
	})

	if !strings.Contains(output, "[1/6]") {
		t.Errorf("Phase() output should contain '[1/6]', got %q", output)
	}
	if !strings.Contains(output, "Migrating schema...") {
		t.Errorf("Phase() output should contain description, got %q", output)
	}
}

func TestPhaseDone(t *testing.T) {
	output := captureStdout(func() {
		PhaseDone(2 * time.Second)
	})

	if !strings.Contains(output, "done") {
		t.Errorf("PhaseDone() output should contain 'done', got %q", output)
	}
	if !strings.Contains(output, "2.0s") {
		t.Errorf("PhaseDone() output should contain duration, got %q", output)
	}
}

func TestPhaseSkipped(t *testing.T) {
	output := captureStdout(func() {
		PhaseSkipped("no views")
	})

	if !strings.Contains(output, "skipped") {
		t.Errorf("PhaseSkipped() output should contain 'skipped', got %q", output)
	}
	if !strings.Contains(output, "no views") {
		t.Errorf("PhaseSkipped() output should contain reason, got %q", output)
	}
}

func TestPhaseFailed(t *testing.T) {
	output := captureStdout(func() {
		PhaseFailed(io.EOF)
	})

	if !strings.Contains(output, "failed") {
		t.Errorf("PhaseFailed() output should contain 'failed', got %q", output)
	}
	if !strings.Contains(output, "Error:") {
		t.Errorf("PhaseFailed() output should contain 'Error:', got %q", output)
	}
}

func TestTableProgress(t *testing.T) {
	output := captureStdout(func() {
		TableProgress("users", 500, 1000)
	})

	if !strings.Contains(output, "users") {
		t.Errorf("TableProgress() output should contain table name, got %q", output)
	}
	if !strings.Contains(output, "50.0%") {
		t.Errorf("TableProgress() output should contain percentage, got %q", output)
	}
}

func TestTableDone(t *testing.T) {
	output := captureStdout(func() {
		TableDone("users", 1000, 500*time.Millisecond)
	})

	if !strings.Contains(output, "✓") {
		t.Errorf("TableDone() output should contain checkmark, got %q", output)
	}
	if !strings.Contains(output, "users") {
		t.Errorf("TableDone() output should contain table name, got %q", output)
	}
	if !strings.Contains(output, "1,000") {
		t.Errorf("TableDone() output should contain formatted row count, got %q", output)
	}
}

func TestSummary(t *testing.T) {
	output := captureStdout(func() {
		Summary(10, 50000, 5*time.Second)
	})

	if !strings.Contains(output, "Migration complete!") {
		t.Errorf("Summary() output should contain 'Migration complete!', got %q", output)
	}
	if !strings.Contains(output, "Tables: 10") {
		t.Errorf("Summary() output should contain table count, got %q", output)
	}
	if !strings.Contains(output, "50,000") {
		t.Errorf("Summary() output should contain formatted row count, got %q", output)
	}
}

func TestConnectionInfo(t *testing.T) {
	output := captureStdout(func() {
		ConnectionInfo("Source", "mysql", "localhost", "3306", "mydb")
	})

	if !strings.Contains(output, "Source") {
		t.Errorf("ConnectionInfo() output should contain label, got %q", output)
	}
	if !strings.Contains(output, "mysql://") {
		t.Errorf("ConnectionInfo() output should contain engine, got %q", output)
	}
	if !strings.Contains(output, "localhost:3306") {
		t.Errorf("ConnectionInfo() output should contain host:port, got %q", output)
	}
	if !strings.Contains(output, "mydb") {
		t.Errorf("ConnectionInfo() output should contain database, got %q", output)
	}
}

func TestConfirm_Yes(t *testing.T) {
	// Save original stdin
	oldStdin := os.Stdin

	// Create a pipe
	r, w, _ := os.Pipe()
	os.Stdin = r

	// Write "y" to stdin
	go func() {
		w.Write([]byte("y\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if !result {
			t.Errorf("Confirm('y') = false, want true")
		}
	})

	// Restore original stdin
	os.Stdin = oldStdin
}

func TestConfirm_No(t *testing.T) {
	// Save original stdin
	oldStdin := os.Stdin

	// Create a pipe
	r, w, _ := os.Pipe()
	os.Stdin = r

	// Write "n" to stdin
	go func() {
		w.Write([]byte("n\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if result {
			t.Errorf("Confirm('n') = true, want false")
		}
	})

	// Restore original stdin
	os.Stdin = oldStdin
}

func TestConfirm_Invalid(t *testing.T) {
	// Save original stdin
	oldStdin := os.Stdin

	// Create a pipe
	r, w, _ := os.Pipe()
	os.Stdin = r

	// Write something other than y/n to stdin
	go func() {
		w.Write([]byte("maybe\n"))
		w.Close()
	}()

	captureStdout(func() {
		result := Confirm("Continue?")
		if result {
			t.Errorf("Confirm('maybe') = true, want false")
		}
	})

	// Restore original stdin
	os.Stdin = oldStdin
}
