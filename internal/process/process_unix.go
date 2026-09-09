//go:build !windows

package process

import (
	"fmt"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
)

type platformState struct{}

func prepareCommand(cmd *exec.Cmd, spec Spec) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if spec.User == "" && spec.Group == "" {
		return nil
	}
	credential := &syscall.Credential{}
	if spec.User == "" {
		current, err := user.Current()
		if err != nil {
			return fmt.Errorf("lookup current user: %w", err)
		}
		uid, err := strconv.ParseUint(current.Uid, 10, 32)
		if err != nil {
			return fmt.Errorf("parse current uid: %w", err)
		}
		credential.Uid = uint32(uid)
	}
	if spec.User != "" {
		value, err := user.Lookup(spec.User)
		if err != nil {
			return fmt.Errorf("lookup user %q: %w", spec.User, err)
		}
		uid, err := strconv.ParseUint(value.Uid, 10, 32)
		if err != nil {
			return fmt.Errorf("parse uid for %q: %w", spec.User, err)
		}
		credential.Uid = uint32(uid)
		groups, err := value.GroupIds()
		if err == nil && len(groups) > 0 {
			if gid, parseErr := strconv.ParseUint(groups[0], 10, 32); parseErr == nil {
				credential.Gid = uint32(gid)
			}
		}
	}
	if spec.Group != "" {
		value, err := user.LookupGroup(spec.Group)
		if err != nil {
			return fmt.Errorf("lookup group %q: %w", spec.Group, err)
		}
		gid, err := strconv.ParseUint(value.Gid, 10, 32)
		if err != nil {
			return fmt.Errorf("parse gid for %q: %w", spec.Group, err)
		}
		credential.Gid = uint32(gid)
	}
	cmd.SysProcAttr.Credential = credential
	return nil
}

func attachProcessTree(h *Handle) {}

func releaseProcessTree(h *Handle) {}

func gracefulStop(h *Handle) error {
	return syscall.Kill(-h.PID(), syscall.SIGTERM)
}

func forceStop(h *Handle) error {
	return syscall.Kill(-h.PID(), syscall.SIGKILL)
}
