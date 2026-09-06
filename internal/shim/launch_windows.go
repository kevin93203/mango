//go:build windows

package shim

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	createNewProcessGroup = 0x00000200
	createNoWindow        = 0x08000000
)

func endpointForState(stateDir string) string {
	absolute, err := filepath.Abs(stateDir)
	if err == nil {
		stateDir = absolute
	}
	stateDir = strings.ToLower(filepath.Clean(stateDir))
	digest := sha256.Sum256([]byte(stateDir))
	return "\\\\.\\pipe\\mango-shim-" + hex.EncodeToString(digest[:])
}

func prepareEndpoint(string) error { return nil }

func configureShimCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNewProcessGroup | createNoWindow}
}

func detachShimOutput(cmd *exec.Cmd, daemonLog string) error {
	if daemonLog == "" {
		return nil
	}
	file, err := os.OpenFile(daemonLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cmd.Stdout = file
	cmd.Stderr = file
	return nil
}
