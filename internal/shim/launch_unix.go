//go:build !windows

package shim

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

func endpointForState(stateDir string) string {
	digest := sha256.Sum256([]byte(stateDir))
	return filepath.Join(endpointRoot(), "s-"+hex.EncodeToString(digest[:])+".sock")
}

func endpointRoot() string {
	if value := os.Getenv("MANGO_SHIM_SOCKET_DIR"); value != "" {
		return value
	}
	return filepath.Join("/tmp", "mango-shim-"+strconv.Itoa(os.Getuid()))
}

func prepareEndpoint(endpoint string) error {
	dir := filepath.Dir(endpoint)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

func configureShimCommand(cmd *exec.Cmd) {}

func detachShimOutput(cmd *exec.Cmd, daemonLog string) error {
	if daemonLog == "" {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
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
