//go:build windows

package process

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"unsafe"

	"golang.org/x/sys/windows"
)

type platformState struct {
	job windows.Handle
}

func prepareCommand(cmd *exec.Cmd, spec Spec) error {
	if spec.User != "" || spec.Group != "" || spec.Identity != nil {
		return fmt.Errorf("run_as user/group is not supported by the Windows process adapter")
	}
	return nil
}

func attachProcessTree(h *Handle, spec Spec) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return fmt.Errorf("create process job: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if spec.Resources != nil {
		if spec.Resources.ProcessLimit > 0 {
			info.BasicLimitInformation.LimitFlags |= windows.JOB_OBJECT_LIMIT_ACTIVE_PROCESS
			info.BasicLimitInformation.ActiveProcessLimit = spec.Resources.ProcessLimit
		}
		if spec.Resources.MemoryBytes > 0 {
			if uint64(uintptr(spec.Resources.MemoryBytes)) != spec.Resources.MemoryBytes {
				_ = windows.CloseHandle(job)
				return fmt.Errorf("job memory limit is too large for this platform")
			}
			info.BasicLimitInformation.LimitFlags |= windows.JOB_OBJECT_LIMIT_JOB_MEMORY
			info.JobMemoryLimit = uintptr(spec.Resources.MemoryBytes)
		}
	}
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("configure process job: %w", err)
	}
	if spec.Resources != nil && spec.Resources.CPUPercent > 0 {
		cpu := jobObjectCPURateControlInformation{
			ControlFlags: jobObjectCPURateControlEnable | jobObjectCPURateControlHardCap,
			CpuRate:      uint32(spec.Resources.CPUPercent) * 100,
		}
		if _, err := windows.SetInformationJobObject(job, windows.JobObjectCpuRateControlInformation, uintptr(unsafe.Pointer(&cpu)), uint32(unsafe.Sizeof(cpu))); err != nil {
			_ = windows.CloseHandle(job)
			return fmt.Errorf("configure process job CPU cap: %w", err)
		}
	}
	processHandle, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		uint32(h.PID()),
	)
	if err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("open child process for job: %w", err)
	}
	err = windows.AssignProcessToJobObject(job, processHandle)
	_ = windows.CloseHandle(processHandle)
	if err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("assign child process to job: %w", err)
	}
	h.mu.Lock()
	h.platform.job = job
	h.mu.Unlock()
	return nil
}

// x/sys/windows does not expose this newer Job Object structure on all
// supported versions, so keep the ABI-sized native definition local.
type jobObjectCPURateControlInformation struct {
	ControlFlags uint32
	CpuRate      uint32
}

const (
	jobObjectCPURateControlEnable  = 0x1
	jobObjectCPURateControlHardCap = 0x4
)

func abortProcessTree(pid int) {
	if err := exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T", "/F").Run(); err == nil {
		return
	}
	process, err := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
	if err == nil {
		_ = windows.TerminateProcess(process, 1)
		_ = windows.CloseHandle(process)
	}
}

func releaseProcessTree(h *Handle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.platform.job != 0 {
		_ = windows.CloseHandle(h.platform.job)
		h.platform.job = 0
	}
}

func gracefulStop(h *Handle) error {
	if h.Process() == nil {
		return nil
	}
	// os.Process.Signal(os.Interrupt) is not implemented on Windows. Return
	// the error so Handle.Stop can immediately terminate the Job Object (or
	// use taskkill as a fallback) instead of waiting for the full timeout.
	return h.Process().Signal(os.Interrupt)
}

func forceStop(h *Handle) error {
	if h.Process() == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.platform.job != 0 {
		return windows.TerminateJobObject(h.platform.job, 1)
	}
	cmd := exec.Command("taskkill", "/PID", strconv.Itoa(h.PID()), "/T", "/F")
	if err := cmd.Run(); err != nil {
		if killErr := h.Process().Kill(); killErr != nil {
			return fmt.Errorf("taskkill: %v; kill: %w", err, killErr)
		}
	}
	return nil
}
