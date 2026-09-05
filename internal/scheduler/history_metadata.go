package scheduler

import (
	"sort"
	"strings"
)

const redactedValue = "<redacted>"

// TaskMetadata is the safe, persisted representation of a task invocation.
// Environment values are intentionally omitted; only explicitly configured
// environment names are retained.
type TaskMetadata struct {
	Command      string
	Args         []string
	WorkingDir   string
	EnvKeys      []string
	ArgsRedacted bool
}

// SafeTaskMetadata snapshots task execution metadata without persisting
// environment values. Arguments are redacted on a best-effort basis using
// sensitive flag names and values from explicitly configured sensitive env
// keys.
func SafeTaskMetadata(command string, args []string, workingDir string, declaredEnv map[string]string) TaskMetadata {
	metadata := TaskMetadata{
		Command:    command,
		Args:       append([]string(nil), args...),
		WorkingDir: workingDir,
	}

	sensitiveValues := make([]string, 0)
	for key := range declaredEnv {
		if sensitiveName(key) {
			sensitiveValues = append(sensitiveValues, declaredEnv[key])
		}
	}
	sort.SliceStable(sensitiveValues, func(i, j int) bool {
		if len(sensitiveValues[i]) == len(sensitiveValues[j]) {
			return sensitiveValues[i] < sensitiveValues[j]
		}
		return len(sensitiveValues[i]) > len(sensitiveValues[j])
	})
	for key := range declaredEnv {
		metadata.EnvKeys = append(metadata.EnvKeys, key)
	}
	sort.Strings(metadata.EnvKeys)

	if command := redactSensitiveValues(metadata.Command, sensitiveValues); command != metadata.Command {
		metadata.Command = command
		metadata.ArgsRedacted = true
	}
	for index, arg := range metadata.Args {
		if strings.Contains(arg, redactedValue) {
			continue
		}
		if name, _, ok := splitFlagValue(arg); ok && sensitiveName(name) {
			metadata.Args[index] = name + "=" + redactedValue
			metadata.ArgsRedacted = true
			continue
		}
		if sensitiveFlag(arg) && index+1 < len(metadata.Args) {
			// Do not consume the next flag when a sensitive option has no
			// separate value; preserving the flag structure is more useful than
			// guessing at a value that is not present.
			if metadata.Args[index+1] != redactedValue && !strings.HasPrefix(metadata.Args[index+1], "-") {
				metadata.Args[index+1] = redactedValue
				metadata.ArgsRedacted = true
			}
			continue
		}
		redacted := redactSensitiveValues(arg, sensitiveValues)
		if redacted != arg {
			metadata.Args[index] = redacted
			metadata.ArgsRedacted = true
		}
	}
	return metadata
}

func splitFlagValue(arg string) (name, value string, ok bool) {
	if !strings.HasPrefix(arg, "-") {
		return "", "", false
	}
	parts := strings.SplitN(arg, "=", 2)
	if len(parts) != 2 || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func sensitiveFlag(arg string) bool {
	if !strings.HasPrefix(arg, "-") || strings.Contains(arg, "=") {
		return false
	}
	return sensitiveName(arg)
}

func sensitiveName(value string) bool {
	normalized := strings.ToLower(strings.TrimLeft(strings.ReplaceAll(value, "-", "_"), "_"))
	if normalized == "p" || normalized == "pw" || normalized == "pwd" {
		return true
	}
	for _, keyword := range []string{
		"access_token", "api_key", "apikey", "authorization", "auth_token",
		"passphrase", "passwd", "password", "private_key", "secret",
		"token", "keyfile_dict", "service_account", "credential",
	} {
		if strings.Contains(normalized, keyword) {
			return true
		}
	}
	return false
}

func redactSensitiveValues(value string, sensitiveValues []string) string {
	for _, sensitiveValue := range sensitiveValues {
		if sensitiveValue != "" {
			value = strings.ReplaceAll(value, sensitiveValue, redactedValue)
		}
	}
	return value
}
