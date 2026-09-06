package daemon

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDatabaseConnectionInfoParsesPostgresURL(t *testing.T) {
	info := databaseConnectionInfo("postgres", "postgresql://reporter:secret@db.example.com:5433/mango?sslmode=require", "")
	if info.ID != "history" || info.Type != "postgres" || info.Host != "db.example.com" || info.Database != "mango" || info.Login != "reporter" || info.Port != 5433 || info.Status != "unknown" {
		t.Fatalf("connection info = %+v", info)
	}
	assertConnectionInfoDoesNotLeakSecret(t, info, "secret", "sslmode")
}

func TestDatabaseConnectionInfoParsesPostgresKeyValueDSN(t *testing.T) {
	info := databaseConnectionInfo("postgres", "host=db.internal port=5434 dbname=mango user=app password='secret value' options='-c search_path=private'", "MANGO_HISTORY_DSN")
	if info.ID != "MANGO_HISTORY_DSN" || info.Host != "db.internal" || info.Database != "mango" || info.Login != "app" || info.Port != 5434 {
		t.Fatalf("connection info = %+v", info)
	}
	assertConnectionInfoDoesNotLeakSecret(t, info, "secret value", "search_path")
}

func TestDatabaseConnectionInfoParsesMySQLDSN(t *testing.T) {
	info := databaseConnectionInfo("mysql", "app:secret@tcp(mysql.internal:3307)/mango?parseTime=true", "")
	if info.Type != "mysql" || info.Host != "mysql.internal" || info.Database != "mango" || info.Login != "app" || info.Port != 3307 {
		t.Fatalf("connection info = %+v", info)
	}
	assertConnectionInfoDoesNotLeakSecret(t, info, "secret", "parseTime")

	info = databaseConnectionInfo("mysql", "app:secret@tcp(mysql.internal)/mango", "")
	if info.Port != 3306 {
		t.Fatalf("default MySQL port = %d, want 3306", info.Port)
	}
}

func TestDatabaseConnectionInfoForSQLite(t *testing.T) {
	info := databaseConnectionInfo("sqlite", "", "")
	if info.ID != "history" || info.Type != "sqlite" || info.Host != "" || info.Database != "" || info.Login != "" || info.Port != 0 {
		t.Fatalf("connection info = %+v", info)
	}
}

func assertConnectionInfoDoesNotLeakSecret(t *testing.T, info interface{}, secrets ...string) {
	t.Helper()
	encoded, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, secret := range secrets {
		if strings.Contains(text, secret) {
			t.Fatalf("connection info leaked %q: %s", secret, text)
		}
	}
}
