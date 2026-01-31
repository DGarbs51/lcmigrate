package format

import "testing"

func TestBytes(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1023, "1023 B"},
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1048576, "1.00 MB"},
		{1572864, "1.50 MB"},
		{1073741824, "1.00 GB"},
		{1099511627776, "1.00 TB"},
		{1125899906842624, "1.00 PB"},
	}
	for _, tt := range tests {
		result := Bytes(tt.input)
		if result != tt.expected {
			t.Errorf("Bytes(%.0f) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestNumber(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1000, "1,000"},
		{1234, "1,234"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1234567, "1,234,567"},
		{12345678, "12,345,678"},
		{123456789, "123,456,789"},
		{1234567890, "1,234,567,890"},
	}
	for _, tt := range tests {
		result := Number(tt.input)
		if result != tt.expected {
			t.Errorf("Number(%d) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello world", 5, "he..."},
		{"hello world", 8, "hello..."},
		{"ab", 2, "ab"},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"a", 1, "a"},
		{"ab", 1, "a"},
		{"", 5, ""},
		{"hello", 0, ""},
	}
	for _, tt := range tests {
		result := Truncate(tt.input, tt.maxLen)
		if result != tt.expected {
			t.Errorf("Truncate(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
		}
	}
}

func TestDuration(t *testing.T) {
	tests := []struct {
		seconds  int64
		expected string
	}{
		{0, "0m"},
		{30, "0m"},
		{60, "1m"},
		{120, "2m"},
		{3600, "1h 0m"},
		{3660, "1h 1m"},
		{7200, "2h 0m"},
		{7320, "2h 2m"},
		{86400, "1d 0h 0m"},
		{90000, "1d 1h 0m"},
		{90060, "1d 1h 1m"},
		{172800, "2d 0h 0m"},
		{180000, "2d 2h 0m"},
	}
	for _, tt := range tests {
		result := Duration(tt.seconds)
		if result != tt.expected {
			t.Errorf("Duration(%d) = %q, want %q", tt.seconds, result, tt.expected)
		}
	}
}
