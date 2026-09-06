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
	Platform  string
	Installed bool
	Detail    string
}

const windowsTaskName = "mango"

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
	root, err := startupWrapperHome(mangoHome)
	if err != nil {
		return err
	}
	wrapperPath := windowsWrapperPath(root)
	if err := os.MkdirAll(filepath.Dir(wrapperPath), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(wrapperPath, []byte(windowsWrapper(executable, mangoHome)), 0o600); err != nil {
		return err
	}
	taskFile, err := os.CreateTemp("", "mango-task-*.xml")
	if err != nil {
		return err
	}
	taskPath := taskFile.Name()
	defer os.Remove(taskPath)
	if _, err := taskFile.Write(windowsTaskXMLBytes(wrapperPath)); err != nil {
		_ = taskFile.Close()
		return err
	}
	if err := taskFile.Close(); err != nil {
		return err
	}
	task := exec.Command("schtasks", "/Create", "/TN", windowsTaskName, "/XML", taskPath, "/F")
	if output, err := task.CombinedOutput(); err != nil {
		return fmt.Errorf("schtasks: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallWindows(mangoHome string) error {
	task := exec.Command("schtasks", "/Delete", "/TN", windowsTaskName, "/F")
	if output, err := task.CombinedOutput(); err != nil && !strings.Contains(strings.ToLower(string(output)), "cannot find") {
		return fmt.Errorf("schtasks: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if root, err := startupWrapperHome(mangoHome); err == nil {
		if err := os.Remove(windowsWrapperPath(root)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func installLaunchd(executable, mangoHome string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "com.mango.daemon.plist")
	if err := os.WriteFile(path, []byte(launchdPlist(executable, mangoHome)), 0o600); err != nil {
		return err
	}
	_ = exec.Command("launchctl", "unload", path).Run()
	if output, err := exec.Command("launchctl", "load", path).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallLaunchd(_ string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, "Library", "LaunchAgents", "com.mango.daemon.plist")
	_ = exec.Command("launchctl", "unload", path).Run()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func statusLaunchd() (Status, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Status{}, err
	}
	path := filepath.Join(home, "Library", "LaunchAgents", "com.mango.daemon.plist")
	_, err = os.Stat(path)
	return Status{Platform: "darwin", Installed: err == nil, Detail: path}, nil
}

func installSystemd(executable, mangoHome string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "mango.service")
	if err := os.WriteFile(path, []byte(systemdUnit(executable, mangoHome)), 0o600); err != nil {
		return err
	}
	_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	if output, err := exec.Command("systemctl", "--user", "enable", "--now", "mango.service").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallSystemd(_ string) error {
	_ = exec.Command("systemctl", "--user", "disable", "--now", "mango.service").Run()
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".config", "systemd", "user", "mango.service")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	return nil
}

func statusSystemd() (Status, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Status{}, err
	}
	path := filepath.Join(home, ".config", "systemd", "user", "mango.service")
	_, err = os.Stat(path)
	return Status{Platform: "linux", Installed: err == nil, Detail: path}, nil
}

func statusWindows() (Status, error) {
	output, err := exec.Command("schtasks", "/Query", "/TN", windowsTaskName).CombinedOutput()
	detail := strings.TrimSpace(string(output))
	if err == nil {
		detail = windowsTaskStatusDetail(detail)
	}
	return Status{Platform: "windows", Installed: err == nil, Detail: detail}, nil
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

func launchdPlist(executable, mangoHome string) string {
	lines := []string{
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
		"<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
		"<plist version=\"1.0\">",
		"<dict>",
		"  <key>Label</key><string>com.mango.daemon</string>",
		"  <key>ProgramArguments</key><array><string>" + xmlEscape(executable) + "</string><string>run</string></array>",
	}
	if mangoHome != "" {
		lines = append(lines, "  <key>EnvironmentVariables</key><dict><key>MANGO_HOME</key><string>"+xmlEscape(mangoHome)+"</string></dict>")
	}
	lines = append(lines,
		"  <key>RunAtLoad</key><true/>",
		"  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>",
		"</dict>",
		"</plist>",
	)
	return strings.Join(lines, "\n") + "\n"
}

func systemdUnit(executable, mangoHome string) string {
	lines := []string{
		"[Unit]",
		"Description=mango service manager",
		"StartLimitIntervalSec=60",
		"StartLimitBurst=5",
		"",
		"[Service]",
		"ExecStart=" + systemdEscape(executable) + " run",
		"Restart=on-failure",
		"RestartSec=2",
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

func windowsWrapperPath(mangoHome string) string {
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

func windowsWrapper(executable, mangoHome string) string {
	lines := []string{"@echo off"}
	if mangoHome != "" {
		lines = append(lines, "set \"MANGO_HOME="+batchEscape(mangoHome)+"\"")
	}
	lines = append(lines, "\""+batchEscape(executable)+"\" run", "")
	return strings.Join(lines, "\r\n")
}

func windowsTaskXML(wrapperPath string) string {
	lines := []string{
		"<?xml version=\"1.0\" encoding=\"UTF-16\"?>",
		"<Task version=\"1.4\" xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\">",
		"  <RegistrationInfo><Description>mango service manager</Description></RegistrationInfo>",
		"  <Triggers><LogonTrigger><Enabled>true</Enabled></LogonTrigger></Triggers>",
		"  <Principals><Principal id=\"Author\"><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>",
		"  <Settings>",
		"    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
		"    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
		"    <StartWhenAvailable>true</StartWhenAvailable>",
		"    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"    <Enabled>true</Enabled>",
		"  </Settings>",
		"  <Actions Context=\"Author\"><Exec><Command>cmd.exe</Command><Arguments>/D /S /C \"" + xmlEscape(wrapperPath) + "\"</Arguments></Exec></Actions>",
		"</Task>",
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

func windowsTaskXMLBytes(wrapperPath string) []byte {
	text := utf16.Encode([]rune(windowsTaskXML(wrapperPath)))
	data := make([]byte, 2+len(text)*2)
	data[0] = 0xff
	data[1] = 0xfe
	for i, value := range text {
		binary.LittleEndian.PutUint16(data[2+i*2:], value)
	}
	return data
}

func batchEscape(value string) string {
	value = strings.ReplaceAll(value, "%", "%%")
	value = strings.ReplaceAll(value, "^", "^^")
	value = strings.ReplaceAll(value, "&", "^&")
	value = strings.ReplaceAll(value, "|", "^|")
	value = strings.ReplaceAll(value, "<", "^<")
	value = strings.ReplaceAll(value, ">", "^>")
	return value
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
