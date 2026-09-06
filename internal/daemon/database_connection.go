package daemon

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/kevin93203/mango/internal/api"
)

func databaseConnectionInfo(driver, dsn, dsnEnv string) api.DatabaseConnectionInfo {
	driver = strings.ToLower(strings.TrimSpace(driver))
	info := api.DatabaseConnectionInfo{
		ID:     "history",
		Type:   driver,
		Status: "unknown",
	}
	if dsnEnv != "" {
		info.ID = dsnEnv
	}
	if dsn == "" {
		return info
	}
	switch driver {
	case "postgres":
		parsePostgresConnectionInfo(&info, dsn)
	case "mysql":
		parseMySQLConnectionInfo(&info, dsn)
	}
	return info
}

func parsePostgresConnectionInfo(info *api.DatabaseConnectionInfo, dsn string) {
	if strings.Contains(dsn, "://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return
		}
		info.Host = parsed.Hostname()
		if parsed.User != nil {
			info.Login = parsed.User.Username()
		}
		info.Database = strings.TrimPrefix(parsed.Path, "/")
		info.Port = parsedPort(parsed.Port(), 5432)
		return
	}
	options := parseKeyValueDSN(dsn)
	info.Host = options["host"]
	info.Login = options["user"]
	info.Database = options["dbname"]
	if info.Database == "" {
		info.Database = options["database"]
	}
	info.Port = parsedPort(options["port"], 5432)
}

func parseMySQLConnectionInfo(info *api.DatabaseConnectionInfo, dsn string) {
	remaining := dsn
	if at := strings.LastIndex(remaining, "@"); at >= 0 {
		userInfo := remaining[:at]
		remaining = remaining[at+1:]
		if colon := strings.Index(userInfo, ":"); colon >= 0 {
			info.Login = decodeDSNPart(userInfo[:colon])
		} else {
			info.Login = decodeDSNPart(userInfo)
		}
	}

	endpoint := remaining
	database := ""
	if slash := strings.Index(remaining, "/"); slash >= 0 {
		endpoint = remaining[:slash]
		database = remaining[slash+1:]
		if query := strings.Index(database, "?"); query >= 0 {
			database = database[:query]
		}
	}
	info.Database = decodeDSNPart(database)
	if strings.HasPrefix(endpoint, "tcp(") && strings.HasSuffix(endpoint, ")") {
		endpoint = strings.TrimSuffix(strings.TrimPrefix(endpoint, "tcp("), ")")
		info.Host, info.Port = parseHostPort(endpoint, 3306)
		return
	}
	if strings.HasPrefix(endpoint, "unix(") && strings.HasSuffix(endpoint, ")") {
		info.Host = strings.TrimSuffix(strings.TrimPrefix(endpoint, "unix("), ")")
		return
	}
	info.Host, info.Port = parseHostPort(endpoint, 3306)
}

func parseHostPort(address string, defaultPort int) (string, int) {
	if address == "" {
		return "", 0
	}
	if host, port, err := net.SplitHostPort(address); err == nil {
		return host, parsedPort(port, defaultPort)
	}
	if strings.Count(address, ":") == 1 {
		parts := strings.SplitN(address, ":", 2)
		return parts[0], parsedPort(parts[1], defaultPort)
	}
	return strings.Trim(address, "[]"), defaultPort
}

func parsedPort(value string, defaultPort int) int {
	if value == "" {
		return defaultPort
	}
	port, err := strconv.Atoi(value)
	if err != nil || port <= 0 || port > 65535 {
		return 0
	}
	return port
}

func decodeDSNPart(value string) string {
	decoded, err := url.QueryUnescape(value)
	if err != nil {
		return value
	}
	return decoded
}

func parseKeyValueDSN(dsn string) map[string]string {
	values := make(map[string]string)
	for index := 0; index < len(dsn); {
		for index < len(dsn) && isDSNSpace(dsn[index]) {
			index++
		}
		if index >= len(dsn) {
			break
		}
		keyStart := index
		for index < len(dsn) && dsn[index] != '=' && !isDSNSpace(dsn[index]) {
			index++
		}
		key := strings.ToLower(dsn[keyStart:index])
		for index < len(dsn) && isDSNSpace(dsn[index]) {
			index++
		}
		if index >= len(dsn) || dsn[index] != '=' {
			for index < len(dsn) && !isDSNSpace(dsn[index]) {
				index++
			}
			continue
		}
		index++
		for index < len(dsn) && isDSNSpace(dsn[index]) {
			index++
		}
		value, next := parseDSNValue(dsn, index)
		if key != "password" && key != "passfile" && key != "sslkey" {
			values[key] = value
		}
		index = next
	}
	return values
}

func parseDSNValue(dsn string, index int) (string, int) {
	if index >= len(dsn) {
		return "", index
	}
	quote := byte(0)
	if dsn[index] == '\'' || dsn[index] == '"' {
		quote = dsn[index]
		index++
	}
	var value strings.Builder
	for index < len(dsn) {
		character := dsn[index]
		if character == '\\' && index+1 < len(dsn) {
			value.WriteByte(dsn[index+1])
			index += 2
			continue
		}
		if quote != 0 {
			if character == quote {
				return value.String(), index + 1
			}
		} else if isDSNSpace(character) {
			return value.String(), index
		}
		value.WriteByte(character)
		index++
	}
	return value.String(), index
}

func isDSNSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}
