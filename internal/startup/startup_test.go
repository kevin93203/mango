package startup

import (
	"runtime"
	"strings"
	"testing"
)

func TestSystemdUnitUsesFailureRestartAndInstanceEnvironment(t *testing.T) {
	unit := systemdUnit("/opt/mango dir/mangod", "/Users/test user/mango")
	for _, want := range []string{
		"Restart=on-failure",
		"RestartSec=2",
		"StartLimitIntervalSec=60",
		"StartLimitBurst=5",
		`Environment="MANGO_HOME=/Users/test user/mango"`,
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("systemd unit does not contain %q:\n%s", want, unit)
		}
	}
	if strings.Contains(unit, "Restart=always") {
		t.Fatal("systemd unit still restarts after successful exit")
	}
}

func TestLaunchdPlistUsesFailureKeepAliveAndInstanceEnvironment(t *testing.T) {
	plist := launchdPlist("/opt/mango/mangod", "/Users/test/mango")
	for _, want := range []string{
		"<key>RunAtLoad</key><true/>",
		"<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>",
		"<key>EnvironmentVariables</key><dict><key>MANGO_HOME</key><string>/Users/test/mango</string></dict>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("launchd plist does not contain %q:\n%s", want, plist)
		}
	}
	if strings.Contains(plist, "<key>KeepAlive</key><true/>") {
		t.Fatal("launchd plist still keeps successful exits alive")
	}
}

func TestWindowsTaskRunsHiddenLauncherAsCurrentUser(t *testing.T) {
	const userID = "S-1-5-21-111111111-222222222-333333333-1001"
	xml := windowsTaskXML(`C:\Users\楊竣凱\App Data\mango\runtime\mangod-start.vbs`, userID)
	for _, want := range []string{
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<Command>wscript.exe</Command>",
		`<Arguments>//B //Nologo &quot;C:\Users\楊竣凱\App Data\mango\runtime\mangod-start.vbs&quot;</Arguments>`,
		"<LogonTrigger><Enabled>true</Enabled><UserId>" + userID + "</UserId></LogonTrigger>",
		"<Principal id=\"Author\"><UserId>" + userID + "</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal>",
	} {
		if !strings.Contains(xml, want) {
			t.Fatalf("Windows task XML does not contain %q:\n%s", want, xml)
		}
	}
	if strings.Contains(xml, "cmd.exe") || strings.Contains(xml, "mangod-start.cmd") {
		t.Fatal("Windows task XML still launches the daemon through a visible cmd.exe wrapper")
	}
}

func TestWindowsLauncherRunsDaemonHiddenAndWaits(t *testing.T) {
	launcher := windowsLauncher(
		`C:\Users\楊竣凱\Program Files\Mango\mangod.exe`,
		`C:\Users\楊竣凱\App Data\mango`,
	)
	for _, want := range []string{
		`Set shell = CreateObject("WScript.Shell")`,
		`exitCode = shell.Run("""C:\Users\楊竣凱\Program Files\Mango\mangod.exe"" run --home ""C:\Users\楊竣凱\App Data\mango""", 0, True)`,
		"WScript.Quit exitCode",
	} {
		if !strings.Contains(launcher, want) {
			t.Fatalf("Windows launcher does not contain %q:\n%s", want, launcher)
		}
	}
	data := windowsLauncherBytes(`C:\Users\楊竣凱\mangod.exe`, "")
	if len(data) < 2 || data[0] != 0xff || data[1] != 0xfe {
		t.Fatalf("Windows launcher is missing a UTF-16LE BOM: %x", data[:min(2, len(data))])
	}
}

func TestCurrentUserIDIsSIDOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows SID check")
	}
	userID, err := currentUserID()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(userID, "S-1-") {
		t.Fatalf("current user ID = %q, want a Windows SID", userID)
	}
}

func TestWindowsTaskXMLUsesUTF16LEWithBOM(t *testing.T) {
	const userID = "S-1-5-21-111111111-222222222-333333333-1001"
	data := windowsTaskXMLBytes(`C:\Users\test user\mango\runtime\mangod-start.vbs`, userID)
	if len(data) < 2 || data[0] != 0xff || data[1] != 0xfe {
		t.Fatalf("Windows task XML is missing a UTF-16LE BOM: %x", data[:min(2, len(data))])
	}
	if !strings.Contains(windowsTaskXML(`C:\Users\test user\mango\runtime\mangod-start.vbs`, userID), `encoding="UTF-16"`) {
		t.Fatal("Windows task XML does not declare UTF-16 encoding")
	}
}

func TestWindowsTaskStatusDetailIncludesFolderAndTask(t *testing.T) {
	output := "Folder: \\\r\nTaskName                                 Next Run Time          Status\r\n========================================= ====================== ===============\r\n\\mango                                  N/A                    Ready\r\n"
	if got := windowsTaskStatusDetail(output); got != `Folder: \, Task: \mango` {
		t.Fatalf("Windows task status detail = %q, want %q", got, `Folder: \, Task: \mango`)
	}
}

func TestWindowsTaskStatusDetailFallsBackToCanonicalTaskName(t *testing.T) {
	if got := windowsTaskStatusDetail("Folder: \\\r\n"); got != `Folder: \, Task: \mango` {
		t.Fatalf("Windows task status detail = %q, want %q", got, `Folder: \, Task: \mango`)
	}
}

func TestWindowsCommandLineArg(t *testing.T) {
	for input, want := range map[string]string{
		`C:\mango`:            `C:\mango`,
		`C:\Users\test user`:  `"C:\Users\test user"`,
		`C:\`:                 `C:\`,
		`C:\path with space\`: `"C:\path with space\\"`,
	} {
		if got := windowsCommandLineArg(input); got != want {
			t.Errorf("windowsCommandLineArg(%q) = %q, want %q", input, got, want)
		}
	}
}
