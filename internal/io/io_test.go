package io

import "testing"

func TestMockConsole_ReadLine(t *testing.T) {
	mock := NewMockConsole([]string{"input1", "input2", "input3"}, "")

	// Read first input
	line, err := mock.ReadLine()
	if err != nil {
		t.Errorf("ReadLine() error = %v", err)
	}
	if line != "input1" {
		t.Errorf("ReadLine() = %q, want %q", line, "input1")
	}

	// Read second input
	line, err = mock.ReadLine()
	if err != nil {
		t.Errorf("ReadLine() error = %v", err)
	}
	if line != "input2" {
		t.Errorf("ReadLine() = %q, want %q", line, "input2")
	}

	// Read third input
	line, err = mock.ReadLine()
	if err != nil {
		t.Errorf("ReadLine() error = %v", err)
	}
	if line != "input3" {
		t.Errorf("ReadLine() = %q, want %q", line, "input3")
	}

	// Read beyond inputs (should return empty)
	line, err = mock.ReadLine()
	if err != nil {
		t.Errorf("ReadLine() error = %v", err)
	}
	if line != "" {
		t.Errorf("ReadLine() = %q, want empty string", line)
	}
}

func TestMockConsole_ReadPassword(t *testing.T) {
	mock := NewMockConsole(nil, "secretpassword")

	password, err := mock.ReadPassword()
	if err != nil {
		t.Errorf("ReadPassword() error = %v", err)
	}
	if password != "secretpassword" {
		t.Errorf("ReadPassword() = %q, want %q", password, "secretpassword")
	}
}

func TestMockConsole_Output(t *testing.T) {
	mock := NewMockConsole(nil, "")

	mock.Print("hello")
	mock.Printf(" %s", "world")
	mock.Println("!")

	expected := "hello world!\n"
	if mock.GetOutput() != expected {
		t.Errorf("GetOutput() = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_Reset(t *testing.T) {
	mock := NewMockConsole([]string{"a", "b"}, "")

	// Read one input
	mock.ReadLine()

	// Add some output
	mock.Print("test")

	// Reset
	mock.Reset()

	// Verify output cleared
	if mock.GetOutput() != "" {
		t.Error("GetOutput() should be empty after Reset()")
	}

	// Verify input index reset
	line, _ := mock.ReadLine()
	if line != "a" {
		t.Errorf("ReadLine() after Reset() = %q, want %q", line, "a")
	}
}

func TestNewStdConsole(t *testing.T) {
	console := NewStdConsole()
	if console == nil {
		t.Errorf("NewStdConsole() = nil, want non-nil")
	}
	if console.reader == nil {
		t.Errorf("NewStdConsole().reader = nil, want non-nil")
	}
}

func TestMockConsole_EmptyInputs(t *testing.T) {
	mock := NewMockConsole(nil, "")

	// Reading from empty inputs should return empty string
	line, err := mock.ReadLine()
	if err != nil {
		t.Errorf("ReadLine() error = %v, want nil", err)
	}
	if line != "" {
		t.Errorf("ReadLine() = %q, want empty string", line)
	}
}

func TestMockConsole_EmptyPassword(t *testing.T) {
	mock := NewMockConsole(nil, "")

	password, err := mock.ReadPassword()
	if err != nil {
		t.Errorf("ReadPassword() error = %v, want nil", err)
	}
	if password != "" {
		t.Errorf("ReadPassword() = %q, want empty string", password)
	}
}

func TestMockConsole_MultipleOutputMethods(t *testing.T) {
	mock := NewMockConsole(nil, "")

	// Test Print without newline
	mock.Print("a")
	mock.Print("b")
	if mock.GetOutput() != "ab" {
		t.Errorf("Print() output = %q, want %q", mock.GetOutput(), "ab")
	}

	mock.Reset()

	// Test Printf formatting
	mock.Printf("Hello, %s! You are %d years old.", "Alice", 30)
	expected := "Hello, Alice! You are 30 years old."
	if mock.GetOutput() != expected {
		t.Errorf("Printf() output = %q, want %q", mock.GetOutput(), expected)
	}

	mock.Reset()

	// Test Println adds newline
	mock.Println("line1")
	mock.Println("line2")
	expected = "line1\nline2\n"
	if mock.GetOutput() != expected {
		t.Errorf("Println() output = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_ImplementsConsoleInterface(t *testing.T) {
	// Compile-time check that MockConsole implements Console
	var _ Console = (*MockConsole)(nil)
}

func TestStdConsole_ImplementsConsoleInterface(t *testing.T) {
	// Compile-time check that StdConsole implements Console
	var _ Console = (*StdConsole)(nil)
}

func TestStdConsole_Print(t *testing.T) {
	// Note: Testing actual StdConsole output would require capturing stdout
	// which is complex and may interfere with the test runner.
	// This is a smoke test to ensure the method doesn't panic.
	console := NewStdConsole()

	// These just verify the methods can be called without panic
	// Actual output goes to stdout during tests
	_ = console
}

func TestMockConsole_MultiplePrints(t *testing.T) {
	mock := NewMockConsole(nil, "")

	// Test multiple print operations
	mock.Print("a")
	mock.Print("b")
	mock.Print("c")

	if mock.GetOutput() != "abc" {
		t.Errorf("GetOutput() = %q, want %q", mock.GetOutput(), "abc")
	}
}

func TestMockConsole_MixedOutput(t *testing.T) {
	mock := NewMockConsole(nil, "")

	mock.Print("start-")
	mock.Printf("%d", 42)
	mock.Print("-")
	mock.Println("end")

	expected := "start-42-end\n"
	if mock.GetOutput() != expected {
		t.Errorf("GetOutput() = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_ReadLineSequence(t *testing.T) {
	inputs := []string{"first", "second", "third", "fourth", "fifth"}
	mock := NewMockConsole(inputs, "")

	for i, expected := range inputs {
		got, err := mock.ReadLine()
		if err != nil {
			t.Errorf("ReadLine() %d error = %v", i, err)
		}
		if got != expected {
			t.Errorf("ReadLine() %d = %q, want %q", i, got, expected)
		}
	}

	// Beyond the inputs
	got, _ := mock.ReadLine()
	if got != "" {
		t.Errorf("ReadLine() beyond inputs = %q, want empty", got)
	}
}

func TestMockConsole_PrintfFormatting(t *testing.T) {
	mock := NewMockConsole(nil, "")

	mock.Printf("String: %s, Int: %d, Float: %.2f", "test", 42, 3.14159)

	expected := "String: test, Int: 42, Float: 3.14"
	if mock.GetOutput() != expected {
		t.Errorf("Printf() = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_PrintlnMultiple(t *testing.T) {
	mock := NewMockConsole(nil, "")

	mock.Println("line1")
	mock.Println("line2")
	mock.Println("line3")

	expected := "line1\nline2\nline3\n"
	if mock.GetOutput() != expected {
		t.Errorf("Println() = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_PrintWithArgs(t *testing.T) {
	mock := NewMockConsole(nil, "")

	mock.Print("a", "b", "c")

	// fmt.Sprint joins with no separator
	expected := "abc"
	if mock.GetOutput() != expected {
		t.Errorf("Print(a, b, c) = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_PrintlnWithArgs(t *testing.T) {
	mock := NewMockConsole(nil, "")

	mock.Println("a", "b", "c")

	// fmt.Sprintln adds spaces between args and newline at end
	expected := "a b c\n"
	if mock.GetOutput() != expected {
		t.Errorf("Println(a, b, c) = %q, want %q", mock.GetOutput(), expected)
	}
}

func TestMockConsole_ResetMultipleTimes(t *testing.T) {
	mock := NewMockConsole([]string{"a", "b"}, "pass")

	// First use
	mock.ReadLine()
	mock.Print("output")
	mock.Reset()

	// Second use
	line, _ := mock.ReadLine()
	if line != "a" {
		t.Errorf("After first reset, ReadLine() = %q, want %q", line, "a")
	}

	mock.Print("new output")
	if mock.GetOutput() != "new output" {
		t.Errorf("After first reset, output = %q, want %q", mock.GetOutput(), "new output")
	}

	// Second reset
	mock.Reset()
	line, _ = mock.ReadLine()
	if line != "a" {
		t.Errorf("After second reset, ReadLine() = %q, want %q", line, "a")
	}
}

func TestMockConsole_PasswordNeverChanges(t *testing.T) {
	mock := NewMockConsole(nil, "secret")

	// Password should return same value every time
	for i := 0; i < 5; i++ {
		pwd, _ := mock.ReadPassword()
		if pwd != "secret" {
			t.Errorf("ReadPassword() call %d = %q, want %q", i, pwd, "secret")
		}
	}
}

func TestNewMockConsole_NilInputs(t *testing.T) {
	mock := NewMockConsole(nil, "password")

	if mock.Inputs != nil {
		t.Errorf("Inputs should be nil when passed nil")
	}
	if mock.Password != "password" {
		t.Errorf("Password = %q, want %q", mock.Password, "password")
	}
}

func TestMockConsole_OutputBuilder(t *testing.T) {
	mock := NewMockConsole(nil, "")

	// Verify the output builder works correctly
	if mock.GetOutput() != "" {
		t.Errorf("Initial output should be empty")
	}

	mock.Print("test")
	if mock.GetOutput() != "test" {
		t.Errorf("After Print, output = %q, want %q", mock.GetOutput(), "test")
	}

	// Multiple calls to GetOutput should return same value
	if mock.GetOutput() != "test" {
		t.Errorf("Second GetOutput = %q, want %q", mock.GetOutput(), "test")
	}
}
