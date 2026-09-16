package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/version"
)

func doctorCommand(layout paths.Layout) error {
	_ = layout
	return doctorCommandWithCaller(layout, call)
}

func doctorCommandWithCaller(layout paths.Layout, caller func(string, interface{}) (ipc.Response, error)) error {
	_ = layout
	response, err := caller("doctor", nil)
	if err != nil {
		return err
	}
	var report api.DoctorReport
	if err := decodeData(response.Data, &report); err != nil {
		return err
	}
	daemonData := report.Daemon
	if daemonData == nil {
		daemonData = map[string]interface{}{}
	}
	daemonAvailable := true
	daemonDatabase := report.HistoryDatabase
	if daemonDatabase.ConnectionInfo.ID == "" {
		daemonDatabase.ConnectionInfo.ID = "history"
	}
	if daemonDatabase.ConnectionInfo.Type == "" {
		daemonDatabase.ConnectionInfo.Type = daemonDatabase.Driver
	}
	if daemonDatabase.ConnectionInfo.Status == "" {
		daemonDatabase.ConnectionInfo.Status = daemonDatabase.Status
	}
	daemonData["history_database"] = daemonDatabase
	daemonHealth := daemonHealthData{Status: "unknown", ConfigErrors: map[string]string{}}
	if err := decodeData(daemonData, &daemonHealth); err != nil {
		return err
	}
	environmentReport := report.Environment
	if environmentReport == nil {
		environmentReport = map[string]string{}
	}
	databaseReport := doctorDatabaseReport(daemonDatabase)
	capabilityReport := report.Capabilities
	if jsonOutput {
		outputReport := map[string]interface{}{
			"platform": report.Platform, "root": report.Root, "registry": report.Registry, "logs": report.Logs,
			"daemon_log": report.DaemonLog, "execution_history": report.ExecutionHistory,
			"history_database": map[string]string{"driver": daemonDatabase.Driver, "location": daemonDatabase.Location},
			"registry_ok":      report.RegistryOK, "registry_error": report.RegistryError,
			"daemon":       daemonData,
			"environment":  environmentReport,
			"database":     databaseReport,
			"capabilities": capabilityReport,
		}
		if report.DaemonExecutable != "" {
			outputReport["daemon_executable"] = report.DaemonExecutable
			daemonData["daemon_binary"] = report.DaemonExecutable
		} else {
			outputReport["daemon_executable_error"] = report.DaemonExecutableError
			daemonData["daemon_binary_error"] = report.DaemonExecutableError
		}
		if report.Startup != nil {
			outputReport["startup"] = report.Startup
			daemonData["startup"] = report.Startup
		} else {
			outputReport["startup_error"] = report.StartupError
			daemonData["startup_error"] = report.StartupError
		}
		daemonData["api_version"] = daemonHealth.Version
		return cliOutput.JSON(outputReport)
	}

	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "mango doctor"))
	printDoctorSection("Environment", []doctorField{
		{Name: "platform", Value: report.Platform},
		{Name: "root", Value: report.Root},
		{Name: "daemon config", Value: environmentReport["daemon_config"]},
		{Name: "registry", Value: report.Registry, Style: doctorStatusStyle(report.RegistryOK, report.RegistryError)},
		{Name: "logs root", Value: report.Logs},
		{Name: "state root", Value: environmentReport["state_root"]},
		{Name: "schedule state", Value: environmentReport["schedule_state"]},
		{Name: "service state", Value: environmentReport["service_state"]},
		{Name: "runtime socket", Value: environmentReport["runtime_socket"]},
	})
	printDoctorSection("Database", []doctorField{
		{Name: "driver", Value: daemonDatabase.Driver},
		{Name: "location", Value: daemonDatabase.Location},
		{Name: "connection id", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.ID)},
		{Name: "conn type", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Type)},
		{Name: "host", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Host)},
		{Name: "database", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Database)},
		{Name: "login", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Login)},
		{Name: "port", Value: displayDoctorPort(daemonDatabase.ConnectionInfo.Port)},
		{Name: "connection", Value: doctorDatabaseConnection(daemonDatabase), Style: doctorDatabaseStyle(daemonDatabase)},
		{Name: "history schema", Value: doctorSchemaStatus(daemonDatabase.Schema), Style: doctorSchemaStyle(daemonDatabase.Schema)},
		{Name: "migration", Value: doctorMigrationStatus(daemonDatabase.Migration), Style: doctorMigrationStyle(daemonDatabase.Migration)},
	})
	capabilityFields := make([]doctorField, 0, len(capabilityReport.Capabilities))
	for name, info := range capabilityReport.Capabilities {
		capabilityFields = append(capabilityFields, doctorField{
			Name: name, Value: info.State + capabilityDetailSuffix(info.Detail),
			Style: capabilityStatusStyle(info.State),
		})
	}
	sort.Slice(capabilityFields, func(i, j int) bool { return capabilityFields[i].Name < capabilityFields[j].Name })
	printDoctorSection("Capabilities", capabilityFields)
	configErrors, configErrorsStyle := doctorConfigErrors(daemonHealth.ConfigErrors, daemonAvailable)
	daemonStatus := daemonHealth.Status
	if daemonStatus == "" {
		daemonStatus = "unknown"
	}
	daemonStatusStyle := cliui.StateStyle(daemonStatus)
	daemonBinary := report.DaemonExecutable
	daemonBinaryStyle := cliui.StyleNone
	if report.DaemonExecutableError != "" {
		daemonBinary = report.DaemonExecutableError
		daemonBinaryStyle = cliui.StyleError
	}
	startupValue := "unknown"
	startupStyle := cliui.StyleWarning
	if report.Startup != nil {
		startupValue = doctorStatusValue(report.Startup.Installed, report.Startup.Detail)
		startupStyle = doctorStatusStyle(report.Startup.Installed, report.Startup.Detail)
		if report.Startup.Installed && !report.Startup.BootEnabled {
			startupValue = "boot disabled: " + report.Startup.Detail
			startupStyle = cliui.StyleWarning
		}
	}
	if report.StartupError != "" {
		startupValue = report.StartupError
		startupStyle = cliui.StyleError
	}
	printDoctorSection("Daemon", []doctorField{
		{Name: "status", Value: daemonStatus, Style: daemonStatusStyle},
		{Name: "pid", Value: doctorPID(daemonHealth, daemonAvailable)},
		{Name: "api version", Value: doctorAPIVersion(daemonHealth, daemonAvailable)},
		{Name: "build", Value: doctorBuild(daemonHealth, daemonAvailable)},
		{Name: "daemon binary", Value: daemonBinary, Style: daemonBinaryStyle},
		{Name: "startup", Value: startupValue, Style: startupStyle},
		{Name: "config errors", Value: configErrors, Style: configErrorsStyle},
	})
	return nil
}

func capabilityDetailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return ": " + detail
}

func capabilityStatusStyle(state string) cliui.Style {
	switch state {
	case api.CapabilitySupported:
		return cliui.StyleSuccess
	case api.CapabilityDegraded:
		return cliui.StyleWarning
	case api.CapabilityUnsupported:
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}

type doctorField struct {
	Name  string
	Value string
	Style cliui.Style
}

func printDoctorSection(title string, fields []doctorField) {
	cliOutput.Println("")
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, title))
	for _, field := range fields {
		cliOutput.Printf("  %-16s %s\n", field.Name, cliOutput.Text(field.Style, field.Value))
	}
}

type doctorDatabaseJSON struct {
	Driver              string                     `json:"driver"`
	Location            string                     `json:"location"`
	Connection          string                     `json:"connection"`
	HistorySchema       string                     `json:"history_schema"`
	ConnectionError     string                     `json:"connection_error,omitempty"`
	HistorySchemaError  string                     `json:"history_schema_error,omitempty"`
	MissingSchemaTables []string                   `json:"missing_schema_tables,omitempty"`
	ConnectionInfo      api.DatabaseConnectionInfo `json:"connection_info"`
	Migration           api.HistoryMigrationHealth `json:"migration"`
}

func doctorDatabaseReport(database api.HistoryDatabaseHealth) doctorDatabaseJSON {
	connection := database.Status
	if connection == "" {
		connection = "unknown"
	}
	schema := database.Schema.Status
	if schema == "" {
		schema = "unknown"
	}
	return doctorDatabaseJSON{
		Driver:              database.Driver,
		Location:            database.Location,
		Connection:          connection,
		HistorySchema:       schema,
		ConnectionError:     database.Error,
		HistorySchemaError:  database.Schema.Error,
		MissingSchemaTables: append([]string(nil), database.Schema.Missing...),
		ConnectionInfo:      database.ConnectionInfo,
		Migration:           database.Migration,
	}
}

func doctorMigrationStatus(migration api.HistoryMigrationHealth) string {
	status := migration.Status
	if status == "" {
		status = "unknown"
	}
	if migration.CurrentVersion > 0 || migration.TargetVersion > 0 {
		status += fmt.Sprintf(" (%d/%d)", migration.CurrentVersion, migration.TargetVersion)
	}
	if migration.Error != "" {
		status += ": " + migration.Error
	}
	return status
}

func doctorMigrationStyle(migration api.HistoryMigrationHealth) cliui.Style {
	switch migration.Status {
	case "complete", "completed", "managed_externally", "not_created":
		return cliui.StyleSuccess
	case "blocked", "failed":
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}

func displayDoctorValue(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func displayDoctorPort(port int) string {
	if port <= 0 {
		return "-"
	}
	return strconv.Itoa(port)
}

func doctorDatabaseConnection(database api.HistoryDatabaseHealth) string {
	status := database.Status
	if status == "" {
		status = "unknown"
	}
	if database.Error != "" {
		return status + ": " + database.Error
	}
	return status
}

func doctorSchemaStatus(schema api.HistorySchemaHealth) string {
	status := schema.Status
	if status == "" {
		status = "unknown"
	}
	if len(schema.Missing) > 0 {
		status += " (missing: " + strings.Join(schema.Missing, ", ") + ")"
	}
	if schema.Error != "" {
		status += ": " + schema.Error
	}
	return status
}

func doctorSchemaStyle(schema api.HistorySchemaHealth) cliui.Style {
	switch schema.Status {
	case "ready":
		return cliui.StyleSuccess
	case "missing", "error":
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}

func doctorConfigErrors(errors map[string]string, available bool) (string, cliui.Style) {
	if !available {
		return "unknown (daemon unavailable)", cliui.StyleWarning
	}
	if len(errors) == 0 {
		return "none", cliui.StyleSuccess
	}
	names := make([]string, 0, len(errors))
	for name := range errors {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, name+": "+errors[name])
	}
	return strings.Join(values, "; "), cliui.StyleError
}

func doctorPID(health daemonHealthData, available bool) string {
	if !available || health.PID <= 0 {
		return "-"
	}
	return strconv.Itoa(health.PID)
}

func doctorAPIVersion(health daemonHealthData, available bool) string {
	if !available || health.Version <= 0 {
		return "-"
	}
	return strconv.Itoa(health.Version)
}

func doctorBuild(health daemonHealthData, available bool) string {
	if !available || health.Build.Version == "" {
		return version.Version
	}
	return health.Build.Version
}

func doctorStatusStyle(ok bool, detail string) cliui.Style {
	if ok {
		return cliui.StyleSuccess
	}
	if detail == "" || detail == "stopped" || detail == "not installed" {
		return cliui.StyleWarning
	}
	return cliui.StyleError
}

func doctorStatusValue(ok bool, detail string) string {
	if detail != "" {
		return detail
	}
	if ok {
		return "ok"
	}
	return "not installed"
}

func doctorDatabaseStyle(database api.HistoryDatabaseHealth) cliui.Style {
	switch database.Status {
	case "connected":
		return cliui.StyleSuccess
	case "disconnected":
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}
