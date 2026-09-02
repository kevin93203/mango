package startup

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"goserve/internal/paths"
)

type Status struct {
	Platform  string
	Installed bool
	Detail    string
}

func Install(layout paths.Layout, executable string) error {
	switch runtime.GOOS {
	case "windows":
		return installWindows(executable)
	case "darwin":
		return installLaunchd(executable)
	case "linux":
		return installSystemd(executable)
	default:
		return fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
}

func Uninstall() error {
	switch runtime.GOOS {
	case "windows":
		return uninstallWindows()
	case "darwin":
		return uninstallLaunchd()
	case "linux":
		return uninstallSystemd()
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

func installWindows(executable string) error {
	task := exec.Command("schtasks", "/Create", "/TN", "goserve", "/TR", fmt.Sprintf("\"%s\" daemon run", executable), "/SC", "ONLOGON", "/F")
	if output, err := task.CombinedOutput(); err != nil {
		return fmt.Errorf("schtasks: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallWindows() error {
	task := exec.Command("schtasks", "/Delete", "/TN", "goserve", "/F")
	if output, err := task.CombinedOutput(); err != nil && !strings.Contains(strings.ToLower(string(output)), "cannot find") {
		return fmt.Errorf("schtasks: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func installLaunchd(executable string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "com.goserve.daemon.plist")
	lines := []string{
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
		"<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
		"<plist version=\"1.0\">",
		"<dict>",
		"  <key>Label</key><string>com.goserve.daemon</string>",
		"  <key>ProgramArguments</key><array><string>" + xmlEscape(executable) + "</string><string>daemon</string><string>run</string></array>",
		"  <key>RunAtLoad</key><true/>",
		"  <key>KeepAlive</key><true/>",
		"</dict>",
		"</plist>",
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		return err
	}
	_ = exec.Command("launchctl", "unload", path).Run()
	if output, err := exec.Command("launchctl", "load", path).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallLaunchd() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, "Library", "LaunchAgents", "com.goserve.daemon.plist")
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
	path := filepath.Join(home, "Library", "LaunchAgents", "com.goserve.daemon.plist")
	_, err = os.Stat(path)
	return Status{Platform: "darwin", Installed: err == nil, Detail: path}, nil
}

func installSystemd(executable string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "goserve.service")
	lines := []string{
		"[Unit]",
		"Description=goserve process manager",
		"",
		"[Service]",
		"ExecStart=" + systemdEscape(executable) + " daemon run",
		"Restart=always",
		"RestartSec=2",
		"",
		"[Install]",
		"WantedBy=default.target",
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		return err
	}
	_ = exec.Command("systemctl", "--user", "daemon-reload").Run()
	if output, err := exec.Command("systemctl", "--user", "enable", "--now", "goserve.service").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func uninstallSystemd() error {
	_ = exec.Command("systemctl", "--user", "disable", "--now", "goserve.service").Run()
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".config", "systemd", "user", "goserve.service")
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
	path := filepath.Join(home, ".config", "systemd", "user", "goserve.service")
	_, err = os.Stat(path)
	return Status{Platform: "linux", Installed: err == nil, Detail: path}, nil
}

func statusWindows() (Status, error) {
	output, err := exec.Command("schtasks", "/Query", "/TN", "goserve").CombinedOutput()
	return Status{Platform: "windows", Installed: err == nil, Detail: strings.TrimSpace(string(output))}, nil
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
