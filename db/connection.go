package db

import (
	"bufio"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type Config struct {
	Engine   string
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

type envDefaults struct {
	Engine   string
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

func getEnvWithFallback(keys ...string) string {
	for _, key := range keys {
		if val := os.Getenv(key); val != "" {
			return val
		}
	}
	return ""
}

func loadEnvDefaults() envDefaults {
	_ = godotenv.Load() // Silently ignore if .env doesn't exist

	return envDefaults{
		Engine:   getEnvWithFallback("DB_ENGINE", "DB_CONNECTION"),
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		Database: getEnvWithFallback("DB_DATABASE", "DB_NAME"),
		User:     getEnvWithFallback("DB_USER", "DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
	}
}

func promptWithDefault(reader *bufio.Reader, prompt, defaultVal string) string {
	cyan := color.New(color.FgCyan).SprintFunc()
	dim := color.New(color.Faint).SprintFunc()

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

func PromptConnectionDetails() Config {
	reader := bufio.NewReader(os.Stdin)
	defaults := loadEnvDefaults()

	bold := color.New(color.Bold).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	dim := color.New(color.Faint).SprintFunc()

	fmt.Println()
	fmt.Printf("  %s\n", bold("🔌 Database Connection Setup"))
	fmt.Printf("  %s\n\n", dim("─────────────────────────────"))

	// Check if .env was loaded
	if defaults.Host != "" || defaults.User != "" {
		fmt.Printf("  %s\n\n", green("✓ Found .env file, using values as defaults"))
	}

	// Engine
	engineDefault := defaults.Engine
	if engineDefault == "" {
		engineDefault = "mysql"
	}
	engine := promptWithDefault(reader, "Database engine (mysql/pgsql)", engineDefault)

	// Host
	hostDefault := defaults.Host
	if hostDefault == "" {
		hostDefault = "localhost"
	}
	host := promptWithDefault(reader, "Host", hostDefault)

	// Port - use .env, or default based on engine
	portDefault := defaults.Port
	if portDefault == "" {
		if engine == "pgsql" {
			portDefault = "5432"
		} else {
			portDefault = "3306"
		}
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

	// Password - show masked if default exists
	passwordDefault := defaults.Password
	passwordDisplay := ""
	if passwordDefault != "" {
		passwordDisplay = "****"
	}
	password := promptWithDefault(reader, "Password", passwordDisplay)
	if password == "****" || password == "" && passwordDefault != "" {
		password = passwordDefault
	}

	fmt.Println()

	return Config{
		Engine:   engine,
		Host:     host,
		Port:     port,
		Database: database,
		User:     user,
		Password: password,
	}
}

func Connect(config Config) (*sql.DB, error) {
	var dsn string
	var driverName string

	bold := color.New(color.Bold).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	red := color.New(color.FgRed).SprintFunc()

	switch config.Engine {
	case "mysql":
		driverName = "mysql"
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			config.User, config.Password, config.Host, config.Port, config.Database)
	case "pgsql":
		driverName = "postgres" // lib/pq uses "postgres" as driver name
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
			url.QueryEscape(config.User), url.QueryEscape(config.Password),
			config.Host, config.Port, config.Database)
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", config.Engine)
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		fmt.Printf("  %s %s\n", red("✗"), err)
		return nil, err
	}

	if err := db.Ping(); err != nil {
		fmt.Printf("  %s %s\n", red("✗"), err)
		return nil, err
	}

	fmt.Printf("  %s %s %s @ %s:%s/%s\n",
		green("✓"),
		bold("Connected to"),
		config.Engine,
		config.Host,
		config.Port,
		config.Database,
	)

	return db, nil
}

func Analyze(conn *sql.DB, config Config) error {
	switch config.Engine {
	case "mysql":
		return AnalyzeMySQL(conn, config.Database)
	case "pgsql":
		return AnalyzePostgres(conn, config.Database)
	default:
		return fmt.Errorf("unsupported database engine: %s", config.Engine)
	}
}
