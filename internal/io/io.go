package io

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/term"
)

// Reader provides input reading capabilities
type Reader interface {
	ReadLine() (string, error)
	ReadPassword() (string, error)
}

// Writer provides output capabilities
type Writer interface {
	Print(args ...interface{})
	Printf(format string, args ...interface{})
	Println(args ...interface{})
}

// Console provides both reading and writing capabilities
type Console interface {
	Reader
	Writer
}

// StdConsole is the default implementation using os.Stdin/Stdout
type StdConsole struct {
	reader *bufio.Reader
}

// NewStdConsole creates a new standard console
func NewStdConsole() *StdConsole {
	return &StdConsole{reader: bufio.NewReader(os.Stdin)}
}

// ReadLine reads a line of input from stdin
func (c *StdConsole) ReadLine() (string, error) {
	input, err := c.reader.ReadString('\n')
	return strings.TrimSpace(input), err
}

// ReadPassword reads a password from stdin without echoing
func (c *StdConsole) ReadPassword() (string, error) {
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // newline after password
	return strings.TrimSpace(string(password)), err
}

// Print writes to stdout without a newline
func (c *StdConsole) Print(args ...interface{}) {
	fmt.Print(args...)
}

// Printf writes formatted output to stdout
func (c *StdConsole) Printf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

// Println writes to stdout with a newline
func (c *StdConsole) Println(args ...interface{}) {
	fmt.Println(args...)
}
