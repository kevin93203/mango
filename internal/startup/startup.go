package startup

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf16"
)

type Status struct {
	Platform    string
	Installed   bool
	BootEnabled bool
	Detail      string
}

const (
	windowsTaskName   = "mango"
	launchdLabel      = "com.mango.daemon"
	systemdUnitName   = "mango.service"
	launchdDaemonPath = "/Library/LaunchDaemons/com.mango.daemon.plist"
)

func Install(executable string, mangoHomes ...string) error {
	mangoHome, err := configuredMangoHome(mangoHomes...)
	if err != nil {
		return err
	}
	switch runtime.GOOS {
	case "windows":
		return installWindows(executable, mangoHome)
	case "darwin":
		return installLaunchd(executable, mangoHome)
	case "linux":
		return installSystemd(executable, mangoHome)
	default:
		return fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
}

func Uninstall(mangoHomes ...string) error {
	mangoHome, err := configuredMangoHome(mangoHomes...)
	if err != nil {
		return err
	}
	switch runtime.GOOS {
	case "windows":
		return uninstallWindows(mangoHome)
	case "darwin":
		return uninstallLaunchd(mangoHome)
	case "linux":
		return uninstallSystemd(mangoHome)
	default:
		return fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
}

func GetStatus() (Status, error) {
	switch runtime.GOOS {
	case "windows":
		return statusWindows()
	case "darwin":
		return statusLaunchd()
	case "linux":
		return statusSystemd()
	default:
		return Status{Platform: runtime.GOOS}, nil
	}
}

func installWindows(executable, mangoHome string) error {
	account, err := currentUser()
	if err != nil {
		return err
	}
	root, err := startupWrapperHome(mangoHome)
	if err != nil {
		return err
	}
	launcherPath := windowsLauncherPath(root)
	if err := os.MkdirAll(filepath.Dir(launcherPath), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(launcherPath, windowsLauncherBytes(executable, mangoHome, account.Home), 0o600); err != nil {
		return err
	}
	taskFile, err := os.CreateTemp("", "mango-task-*.xml")
	if err != nil {
		return err
	}
	taskPath := taskFile.Name()
	defer os.Remove(taskPath)
	if _, err := taskFile.Write(windowsTaskXMLBytes(launcherPath, account.UID)); err != nil {
		_ = taskFile.Close()
		return err
	}
	if err := taskFile.Close(); err != nil {
		return err
	}
	if err := runPrivilegedCommand("schtasks", "/Create", "/TN", windowsTaskName, "/XML", taskPath, "/F"); err != nil {
		return err
	}
	return nil
}

func uninstallWindows(mangoHome string) error {
	if status, _ := statusWindows(); status.Installed {
		if err := runPrivilegedCommand("schtasks", "/Delete", "/TN", windowsTaskName, "/F"); err != nil {
			return err
		}
	}
	if root, err := startupWrapperHome(mangoHome); err == nil {
		for _, path := range []string{windowsLauncherPath(root), legacyWindowsWrapperPath(root)} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func installLaunchd(executable, mangoHome string) error {
	account, err := currentUser()
	if err != nil {
		return err
	}
	legacyPath := filepath.Join(account.Home, "Library", "LaunchAgents", launchdLabel+".plist")
	path, cleanup, err := writeTemporaryStartupFile("mango-launchd-*.plist", []byte(launchdPlist(executable, mangoHome, account.Name, account.Home)))
	if err != nil {
		return err
	}
	defer cleanup()

	if err := runPrivilegedCommand("install", "-o", "root", "-g", "wheel", "-m", "0644", path, launchdDaemonPath); err != nil {
		return fmt.Errorf("install launch daemon: %w", err)
	}
	_ = exec.Command("launchctl", "bootout", "gui/"+account.UID, legacyPath).Run()
	_ = exec.Command("launchctl", "unload", legacyPath).Run()
	if err := os.Remove(legacyPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	_ = runPrivilegedCommand("launchctl", "bootout", "system/"+launchdLabel)
	if err := runPrivilegedCommand("launchctl", "bootstrap", "system", launchdDaemonPath); err != nil {
		return fmt.Errorf("bootstrap launch daemon: %w", err)
	}
	return nil
}

func uninstallLaunchd(_ string) error {
	account, err := currentUser()
	if err != nil {
		return err
	}
	if _, statErr := os.Stat(launchdDaemonPath); statErr == nil {
		_ = runPrivilegedCommand("launchctl", "bootout", "system/"+launchdLabel)
		if err := runPrivilegedCommand("rm", "-f", launchdDaemonPath); err != nil {
			return fmt.Errorf("remove launch daemon: %w", err)
		}
	}
	legacyPath := filepath.Join(account.Home, "Library", "LaunchAgents", launchdLabel+".plist")
	_ = exec.Command("launchctl", "bootout", "gui/"+account.UID, legacyPath).Run()
	_ = exec.Command("launchctl", "unload", legacyPath).Run()
	if err := os.Remove(legacyPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func statusLaunchd() (Status, error) {
	account, err := currentUser()
	if err != nil {
		return Status{}, err
	}
	legacyPath := filepath.Join(account.Home, "Library", "LaunchAgents", launchdLabel+".plist")
	if _, err := os.Stat(launchdDaemonPath); err != nil {
		if _, legacyErr := os.Stat(legacyPath); legacyErr == nil {
			return Status{Platform: "darwin", Installed: true, Detail: legacyPath}, nil
		}
		return Status{Platform: "darwin", Detail: launchdDaemonPath}, nil
	}
	bootEnabled := exec.Command("launchctl", "print", "system/"+launchdLabel).Run() == nil
	return Status{Platform: "darwin", Installed: true, BootEnabled: bootEnabled, Detail: launchdDaemonPath}, nil
}

func installSystemd(executable, mangoHome string) error {
	account, err := currentUser()
	if err != nil {
		return err
	}
	dir := filepath.Join(account.Home, ".config", "systemd", "user")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, systemdUnitName)
	if err := os.WriteFile(path, []byte(systemdUnit(executable, mangoHome, account.Home)), 0o600); err != nil {
		return err
	}
	if !userLingerEnabled(account.UID) {
		if err := runPrivilegedCommand("loginctl", "enable-linger", account.UID); err != nil {
			return fmt.Errorf("enable user lingering: %w", err)
		}
	}
	if output, err := exec.Command("systemctl", "--user", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if output, err := exec.Command("systemctl", "--user", "enable", "--now", systemdUnitName).CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallSystemd(_ string) error {
	_ = exec.Command("systemctl", "--user", "disable", "--now", systemdUnitName).Run()
	account, err := currentUser()
	if err != nil {
		return err
	}
	path := filepath.Join(account.Home, ".config", "systemd", "user", systemdUnitName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	// User lingering is intentionally shared state. Removing Mango must not
	// disable it for Docker, Podman, or another user service.
	return nil
}

func statusSystemd() (Status, error) {
	account, err := currentUser()
	if err != nil {
		return Status{}, err
	}
	path := filepath.Join(account.Home, ".config", "systemd", "user", systemdUnitName)
	if _, err := os.Stat(path); err != nil {
		return Status{Platform: "linux", Detail: path}, nil
	}
	enabled := exec.Command("systemctl", "--user", "is-enabled", systemdUnitName).Run() == nil
	linger := userLingerEnabled(account.UID)
	return Status{Platform: "linux", Installed: true, BootEnabled: enabled && linger, Detail: path}, nil
}

func userLingerEnabled(uid string) bool {
	return strings.TrimSpace(string(commandOutput("loginctl", "show-user", uid, "-p", "Linger", "--value"))) == "yes"
}

func statusWindows() (Status, error) {
	output, err := exec.Command("schtasks", "/Query", "/TN", windowsTaskName).CombinedOutput()
	detail := strings.TrimSpace(string(output))
	bootEnabled := false
	if err == nil {
		detail = windowsTaskStatusDetail(detail)
		xmlOutput, xmlErr := exec.Command("schtasks", "/Query", "/TN", windowsTaskName, "/XML").CombinedOutput()
		bootEnabled = xmlErr == nil && strings.Contains(string(xmlOutput), "<BootTrigger>") && strings.Contains(string(xmlOutput), "<LogonType>S4U</LogonType>")
	}
	return Status{Platform: "windows", Installed: err == nil, BootEnabled: bootEnabled, Detail: detail}, nil
}

func writeTemporaryStartupFile(pattern string, data []byte) (string, func(), error) {
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", func() {}, err
	}
	path := file.Name()
	cleanup := func() { _ = os.Remove(path) }
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		cleanup()
		return "", func() {}, err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		cleanup()
		return "", func() {}, err
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return path, cleanup, nil
}

func commandOutput(name string, args ...string) []byte {
	output, _ := exec.Command(name, args...).CombinedOutput()
	return output
}

func windowsTaskStatusDetail(output string) string {
	folder := ""
	taskName := ""
	for _, line := range strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(line), "folder:") {
			folder = line
			continue
		}
		if strings.HasPrefix(line, `\`) && !strings.Contains(line, "=") {
			fields := strings.Fields(line)
			if len(fields) > 0 {
				taskName = fields[0]
			}
		}
	}
	if folder != "" && taskName != "" {
		return folder + ", Task: " + taskName
	}
	if folder != "" {
		return folder + `, Task: \` + windowsTaskName
	}
	return output
}

func configuredMangoHome(values ...string) (string, error) {
	value := os.Getenv("MANGO_HOME")
	if len(values) > 0 {
		value = values[0]
	}
	if value == "" {
		return "", nil
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func launchdPlist(executable, mangoHome string, user ...string) string {
	userName := ""
	home := ""
	if len(user) > 0 {
		userName = user[0]
	}
	if len(user) > 1 {
		home = user[1]
	}
	lines := []string{
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
		"<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
		"<plist version=\"1.0\">",
		"<dict>",
		"  <key>Label</key><string>" + xmlEscape(launchdLabel) + "</string>",
		"  <key>ProgramArguments</key><array><string>" + xmlEscape(executable) + "</string><string>run</string></array>",
	}
	if userName != "" {
		lines = append(lines, "  <key>UserName</key><string>"+xmlEscape(userName)+"</string>")
	}
	if home != "" || mangoHome != "" {
		environment := "  <key>EnvironmentVariables</key><dict>"
		if home != "" {
			environment += "<key>HOME</key><string>" + xmlEscape(home) + "</string>"
		}
		if mangoHome != "" {
			environment += "<key>MANGO_HOME</key><string>" + xmlEscape(mangoHome) + "</string>"
		}
		environment += "</dict>"
		lines = append(lines, environment)
	}
	lines = append(lines,
		"  <key>RunAtLoad</key><true/>",
		"  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>",
		"</dict>",
		"</plist>",
	)
	return strings.Join(lines, "\n") + "\n"
}

func systemdUnit(executable, mangoHome string, home ...string) string {
	lines := []string{
		"[Unit]",
		"Description=mango service manager",
		"StartLimitIntervalSec=60",
		"StartLimitBurst=5",
		"",
		"[Service]",
		"Type=simple",
		"ExecStart=" + systemdEscape(executable) + " run",
		"Restart=on-failure",
		"RestartSec=2",
		"Delegate=yes",
		"KillMode=control-group",
	}
	if len(home) > 0 && home[0] != "" {
		lines = append(lines, "Environment=\"HOME="+systemdQuote(home[0])+"\"")
	}
	if mangoHome != "" {
		lines = append(lines, "Environment=\"MANGO_HOME="+systemdQuote(mangoHome)+"\"")
	}
	lines = append(lines,
		"",
		"[Install]",
		"WantedBy=default.target",
	)
	return strings.Join(lines, "\n") + "\n"
}

func systemdQuote(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	value = strings.ReplaceAll(value, "%", "%%")
	return value
}

func windowsLauncherPath(mangoHome string) string {
	return filepath.Join(mangoHome, "runtime", "mangod-start.vbs")
}

func legacyWindowsWrapperPath(mangoHome string) string {
	return filepath.Join(mangoHome, "runtime", "mangod-start.cmd")
}

func startupWrapperHome(mangoHome string) (string, error) {
	if mangoHome != "" {
		return mangoHome, nil
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(config, "mango"), nil
}

func windowsLauncher(executable, mangoHome string, home ...string) string {
	command := windowsCommandLineArg(executable) + " run"
	if mangoHome != "" {
		command += " --home " + windowsCommandLineArg(mangoHome)
	}
	command = strings.ReplaceAll(command, `"`, `""`)
	hasHome := len(home) > 0 && home[0] != ""
	lines := []string{"Option Explicit"}
	if hasHome {
		lines = append(lines, "Dim shell, environment, exitCode")
	} else {
		lines = append(lines, "Dim shell, exitCode")
	}
	lines = append(lines, `Set shell = CreateObject("WScript.Shell")`)
	if hasHome {
		profile := windowsVBScriptString(home[0])
		roaming := windowsVBScriptString(windowsProfilePath(home[0], "AppData\\Roaming"))
		local := windowsVBScriptString(windowsProfilePath(home[0], "AppData\\Local"))
		lines = append(lines,
			`Set environment = shell.Environment("Process")`,
			`environment("HOME") = "`+profile+`"`,
			`environment("USERPROFILE") = "`+profile+`"`,
			`environment("APPDATA") = "`+roaming+`"`,
			`environment("LOCALAPPDATA") = "`+local+`"`,
		)
	}
	lines = append(lines,
		`exitCode = shell.Run("`+command+`", 0, True)`,
		"WScript.Quit exitCode",
		"",
	)
	return strings.Join(lines, "\r\n")
}

func windowsLauncherBytes(executable, mangoHome string, home ...string) []byte {
	return utf16LEBytes(windowsLauncher(executable, mangoHome, home...))
}

func windowsProfilePath(home, suffix string) string {
	return strings.TrimRight(home, `\/`) + `\` + suffix
}

func windowsVBScriptString(value string) string {
	return strings.ReplaceAll(value, `"`, `""`)
}

func windowsTaskXML(launcherPath, userID string) string {
	escapedUserID := xmlEscape(userID)
	arguments := "//B //Nologo " + windowsCommandLineArg(launcherPath)
	lines := []string{
		"<?xml version=\"1.0\" encoding=\"UTF-16\"?>",
		"<Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">",
		"  <RegistrationInfo><Description>mango service manager</Description></RegistrationInfo>",
		"  <Triggers><BootTrigger><Enabled>true</Enabled></BootTrigger></Triggers>",
		"  <Principals><Principal id=\"Author\"><UserId>" + escapedUserID + "</UserId><LogonType>S4U</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>",
		"  <Settings>",
		"    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
		"    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
		"    <StartWhenAvailable>true</StartWhenAvailable>",
		"    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"    <Enabled>true</Enabled>",
		"  </Settings>",
		"  <Actions Context=\"Author\"><Exec><Command>wscript.exe</Command><Arguments>" + xmlEscape(arguments) + "</Arguments></Exec></Actions>",
		"</Task>",
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

func windowsTaskXMLBytes(launcherPath, userID string) []byte {
	return utf16LEBytes(windowsTaskXML(launcherPath, userID))
}

func utf16LEBytes(value string) []byte {
	text := utf16.Encode([]rune(value))
	data := make([]byte, 2+len(text)*2)
	data[0] = 0xff
	data[1] = 0xfe
	for i, value := range text {
		binary.LittleEndian.PutUint16(data[2+i*2:], value)
	}
	return data
}

func windowsCommandLineArg(value string) string {
	if value != "" && !strings.ContainsAny(value, " \t\n\v\"") {
		return value
	}
	var quoted strings.Builder
	quoted.WriteByte('"')
	backslashes := 0
	for _, char := range value {
		if char == '\\' {
			backslashes++
			continue
		}
		if char == '"' {
			quoted.WriteString(strings.Repeat("\\", backslashes*2+1))
			quoted.WriteRune(char)
		} else {
			quoted.WriteString(strings.Repeat("\\", backslashes))
			quoted.WriteRune(char)
		}
		backslashes = 0
	}
	quoted.WriteString(strings.Repeat("\\", backslashes*2))
	quoted.WriteByte('"')
	return quoted.String()
}

func xmlEscape(value string) string {
	value = strings.ReplaceAll(value, "&", "&amp;")
	value = strings.ReplaceAll(value, "<", "&lt;")
	value = strings.ReplaceAll(value, ">", "&gt;")
	return strings.ReplaceAll(value, "\"", "&quot;")
}

func systemdEscape(value string) string {
	if strings.ContainsAny(value, " \t") {
		return "\"" + strings.ReplaceAll(value, "\"", "\\\"") + "\""
	}
	return value
}
