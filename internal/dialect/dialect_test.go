package dialect

import "testing"

func TestNew(t *testing.T) {
	tests := []struct {
		engine   string
		wantName string
		wantNil  bool
	}{
		{"mysql", "mysql", false},
		{"pgsql", "pgsql", false},
		{"unknown", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		d := New(tt.engine)
		if tt.wantNil {
			if d != nil {
				t.Errorf("New(%q) = %v, want nil", tt.engine, d)
			}
		} else {
			if d == nil {
				t.Errorf("New(%q) = nil, want non-nil", tt.engine)
			} else if d.Name() != tt.wantName {
				t.Errorf("New(%q).Name() = %q, want %q", tt.engine, d.Name(), tt.wantName)
			}
		}
	}
}

func TestMySQLDialect_QuoteIdentifier(t *testing.T) {
	d := &MySQLDialect{}
	tests := []struct {
		input    string
		expected string
	}{
		{"users", "`users`"},
		{"user`name", "`user``name`"},
		{"table", "`table`"},
		{"", "``"},
		{"column_name", "`column_name`"},
	}
	for _, tt := range tests {
		result := d.QuoteIdentifier(tt.input)
		if result != tt.expected {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestPostgresDialect_QuoteIdentifier(t *testing.T) {
	d := &PostgresDialect{}
	tests := []struct {
		input    string
		expected string
	}{
		{"users", `"users"`},
		{`user"name`, `"user""name"`},
		{"table", `"table"`},
		{"", `""`},
		{"column_name", `"column_name"`},
	}
	for _, tt := range tests {
		result := d.QuoteIdentifier(tt.input)
		if result != tt.expected {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMySQLDialect_QuoteLiteral(t *testing.T) {
	d := &MySQLDialect{}
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
		{"don't stop", "'don''t stop'"},
	}
	for _, tt := range tests {
		result := d.QuoteLiteral(tt.input)
		if result != tt.expected {
			t.Errorf("QuoteLiteral(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestPostgresDialect_QuoteLiteral(t *testing.T) {
	d := &PostgresDialect{}
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
		{"don't stop", "'don''t stop'"},
	}
	for _, tt := range tests {
		result := d.QuoteLiteral(tt.input)
		if result != tt.expected {
			t.Errorf("QuoteLiteral(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMySQLDialect_Placeholder(t *testing.T) {
	d := &MySQLDialect{}
	for i := 1; i <= 10; i++ {
		if d.Placeholder(i) != "?" {
			t.Errorf("Placeholder(%d) = %q, want ?", i, d.Placeholder(i))
		}
	}
}

func TestPostgresDialect_Placeholder(t *testing.T) {
	d := &PostgresDialect{}
	tests := []struct {
		position int
		expected string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
		{100, "$100"},
	}
	for _, tt := range tests {
		result := d.Placeholder(tt.position)
		if result != tt.expected {
			t.Errorf("Placeholder(%d) = %q, want %q", tt.position, result, tt.expected)
		}
	}
}

func TestMySQLDialect_PlaceholderStyle(t *testing.T) {
	d := &MySQLDialect{}
	if d.PlaceholderStyle() != PlaceholderQuestion {
		t.Errorf("PlaceholderStyle() = %v, want PlaceholderQuestion", d.PlaceholderStyle())
	}
}

func TestPostgresDialect_PlaceholderStyle(t *testing.T) {
	d := &PostgresDialect{}
	if d.PlaceholderStyle() != PlaceholderPositional {
		t.Errorf("PlaceholderStyle() = %v, want PlaceholderPositional", d.PlaceholderStyle())
	}
}

func TestMySQLDialect_FKChecksSQL(t *testing.T) {
	d := &MySQLDialect{}
	if d.DisableFKChecksSQL() != "SET FOREIGN_KEY_CHECKS = 0" {
		t.Errorf("DisableFKChecksSQL() = %q, want SET FOREIGN_KEY_CHECKS = 0", d.DisableFKChecksSQL())
	}
	if d.EnableFKChecksSQL() != "SET FOREIGN_KEY_CHECKS = 1" {
		t.Errorf("EnableFKChecksSQL() = %q, want SET FOREIGN_KEY_CHECKS = 1", d.EnableFKChecksSQL())
	}
}

func TestPostgresDialect_FKChecksSQL(t *testing.T) {
	d := &PostgresDialect{}
	if d.DisableFKChecksSQL() != "SET session_replication_role = replica" {
		t.Errorf("DisableFKChecksSQL() = %q, want SET session_replication_role = replica", d.DisableFKChecksSQL())
	}
	if d.EnableFKChecksSQL() != "SET session_replication_role = DEFAULT" {
		t.Errorf("EnableFKChecksSQL() = %q, want SET session_replication_role = DEFAULT", d.EnableFKChecksSQL())
	}
}

func TestMySQLDialect_SupportsSequences(t *testing.T) {
	d := &MySQLDialect{}
	if d.SupportsSequences() {
		t.Error("SupportsSequences() = true, want false")
	}
}

func TestPostgresDialect_SupportsSequences(t *testing.T) {
	d := &PostgresDialect{}
	if !d.SupportsSequences() {
		t.Error("SupportsSequences() = false, want true")
	}
}

func TestMySQLDialect_DefaultFKAction(t *testing.T) {
	d := &MySQLDialect{}
	if d.DefaultFKAction() != "RESTRICT" {
		t.Errorf("DefaultFKAction() = %q, want RESTRICT", d.DefaultFKAction())
	}
}

func TestPostgresDialect_DefaultFKAction(t *testing.T) {
	d := &PostgresDialect{}
	if d.DefaultFKAction() != "NO ACTION" {
		t.Errorf("DefaultFKAction() = %q, want NO ACTION", d.DefaultFKAction())
	}
}

func TestMySQLDialect_Name(t *testing.T) {
	d := &MySQLDialect{}
	if d.Name() != "mysql" {
		t.Errorf("Name() = %q, want mysql", d.Name())
	}
}

func TestPostgresDialect_Name(t *testing.T) {
	d := &PostgresDialect{}
	if d.Name() != "pgsql" {
		t.Errorf("Name() = %q, want pgsql", d.Name())
	}
}
