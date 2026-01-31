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
