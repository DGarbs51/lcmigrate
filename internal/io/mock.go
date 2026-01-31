package io

import (
	"fmt"
	"strings"
)

// MockConsole is a test double for Console
type MockConsole struct {
	Inputs   []string        // Inputs to return from ReadLine (in order)
	Password string          // Password to return from ReadPassword
	Output   strings.Builder // Captured output
	inputIdx int
}

// NewMockConsole creates a new mock console with the given inputs
func NewMockConsole(inputs []string, password string) *MockConsole {
	return &MockConsole{
		Inputs:   inputs,
		Password: password,
	}
}

// ReadLine returns the next input from the Inputs slice
func (m *MockConsole) ReadLine() (string, error) {
	if m.inputIdx >= len(m.Inputs) {
		return "", nil
	}
	result := m.Inputs[m.inputIdx]
	m.inputIdx++
	return result, nil
}

// ReadPassword returns the configured password
func (m *MockConsole) ReadPassword() (string, error) {
	return m.Password, nil
}

// Print captures the output
func (m *MockConsole) Print(args ...interface{}) {
	m.Output.WriteString(fmt.Sprint(args...))
}

// Printf captures the formatted output
func (m *MockConsole) Printf(format string, args ...interface{}) {
	m.Output.WriteString(fmt.Sprintf(format, args...))
}

// Println captures the output with newline
func (m *MockConsole) Println(args ...interface{}) {
	m.Output.WriteString(fmt.Sprintln(args...))
}

// GetOutput returns all captured output
func (m *MockConsole) GetOutput() string {
	return m.Output.String()
}

// Reset clears the captured output and input index
func (m *MockConsole) Reset() {
	m.Output.Reset()
	m.inputIdx = 0
}
