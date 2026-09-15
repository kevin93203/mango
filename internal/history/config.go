package history

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/paths"
)

// ResolveConfig applies Mango's daemon database defaults and path rules to a
// database configuration before opening a repository.
func ResolveConfig(layout paths.Layout, database config.DatabaseConfig) (Config, error) {
	if err := database.Validate(); err != nil {
		return Config{}, err
	}
	driver := strings.ToLower(strings.TrimSpace(database.Driver))
	if driver == "" {
		driver = config.DefaultHistoryDatabaseDriver
	}
	dsn := database.DSN
	if database.DSNEnv != "" {
		dsn = os.Getenv(database.DSNEnv)
		if dsn == "" {
			return Config{}, fmt.Errorf("history database environment variable %q is empty", database.DSNEnv)
		}
	}
	path := database.Path
	if driver == "sqlite" {
		if path == "" {
			path = filepath.Join(layout.State, "history.db")
		} else if !filepath.IsAbs(path) {
			path = filepath.Join(layout.Root, path)
		}
	}
	return Config{Driver: driver, Path: path, DSN: dsn}, nil
}
