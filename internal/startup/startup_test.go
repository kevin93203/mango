package startup

import (
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

func TestWindowsTaskUsesIgnoreNewAndWrapper(t *testing.T) {
	xml := windowsTaskXML(`C:\Users\test user\mango\runtime\mangod-start.cmd`)
	for _, want := range []string{
		"<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
		"<Command>cmd.exe</Command>",
		"mangod-start.cmd",
	} {
		if !strings.Contains(xml, want) {
			t.Fatalf("Windows task XML does not contain %q:\n%s", want, xml)
		}
	}
}

func TestWindowsTaskXMLUsesUTF16LEWithBOM(t *testing.T) {
	data := windowsTaskXMLBytes(`C:\Users\test user\mango\runtime\mangod-start.cmd`)
	if len(data) < 2 || data[0] != 0xff || data[1] != 0xfe {
		t.Fatalf("Windows task XML is missing a UTF-16LE BOM: %x", data[:min(2, len(data))])
	}
	if !strings.Contains(windowsTaskXML(`C:\Users\test user\mango\runtime\mangod-start.cmd`), `encoding="UTF-16"`) {
		t.Fatal("Windows task XML does not declare UTF-16 encoding")
	}
}

func TestWindowsWrapperSetsMangoHome(t *testing.T) {
	wrapper := windowsWrapper(`C:\Program Files\Mango\mangod.exe`, `C:\Users\test user\mango`)
	if !strings.Contains(wrapper, `set "MANGO_HOME=C:\Users\test user\mango"`) {
		t.Fatalf("wrapper does not set MANGO_HOME:\n%s", wrapper)
	}
	if !strings.Contains(wrapper, `"C:\Program Files\Mango\mangod.exe" run`) {
		t.Fatalf("wrapper does not launch mangod:\n%s", wrapper)
	}
}
