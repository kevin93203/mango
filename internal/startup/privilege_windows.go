//go:build windows

package startup

import (
	"fmt"
	"os/exec"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	shellExecuteMaskNoCloseProcess = 0x00000040
	shellExecuteShowHidden         = 0
	waitObjectInfinite             = 0xffffffff
)

var (
	modShell32          = windows.NewLazySystemDLL("shell32.dll")
	procShellExecuteExW = modShell32.NewProc("ShellExecuteExW")
)

type shellExecuteInfo struct {
	Size       uint32
	Mask       uint32
	Window     windows.Handle
	Verb       *uint16
	File       *uint16
	Parameters *uint16
	Directory  *uint16
	Show       int32
	Instance   windows.Handle
	IDList     uintptr
	Class      *uint16
	ClassKey   windows.Handle
	HotKey     uint32
	Icon       windows.Handle
	Process    windows.Handle
}

func runPrivilegedCommand(name string, args ...string) error {
	if windows.GetCurrentProcessToken().IsElevated() {
		return runCommand(name, args...)
	}

	verb, err := windows.UTF16PtrFromString("runas")
	if err != nil {
		return err
	}
	file, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	parameters, err := windows.UTF16PtrFromString(strings.Join(mapWindowsArgs(args), " "))
	if err != nil {
		return err
	}

	info := shellExecuteInfo{
		Size:       uint32(unsafe.Sizeof(shellExecuteInfo{})),
		Mask:       shellExecuteMaskNoCloseProcess,
		Verb:       verb,
		File:       file,
		Parameters: parameters,
		Show:       shellExecuteShowHidden,
	}
	result, _, callErr := procShellExecuteExW.Call(uintptr(unsafe.Pointer(&info)))
	if result == 0 {
		if callErr != nil && callErr != windows.ERROR_SUCCESS {
			return fmt.Errorf("elevate %s: %w", name, callErr)
		}
		return fmt.Errorf("elevate %s: UAC request was cancelled", name)
	}
	if info.Process == 0 {
		return fmt.Errorf("elevate %s: elevated process handle unavailable", name)
	}
	defer windows.CloseHandle(info.Process)
	if _, err := windows.WaitForSingleObject(info.Process, waitObjectInfinite); err != nil {
		return fmt.Errorf("wait for elevated %s: %w", name, err)
	}
	var exitCode uint32
	if err := windows.GetExitCodeProcess(info.Process, &exitCode); err != nil {
		return fmt.Errorf("read elevated %s exit code: %w", name, err)
	}
	if exitCode != 0 {
		return fmt.Errorf("elevated %s exited with code %d", name, exitCode)
	}
	return nil
}

func runCommand(name string, args ...string) error {
	command := exec.Command(name, args...)
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func mapWindowsArgs(args []string) []string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, windowsCommandLineArg(arg))
	}
	return quoted
}
