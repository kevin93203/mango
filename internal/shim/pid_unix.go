//go:build !windows

package shim

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

func processIsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	if runtime.GOOS != "linux" {
		return true
	}
	_, alive := linuxProcessStat(pid)
	return alive
}

func processIsAliveWithToken(pid int, token string) bool {
	if !processIsAlive(pid) {
		return false
	}
	if token == "" || strings.HasPrefix(token, "pid:") || runtime.GOOS != "linux" {
		return true
	}
	current, alive := linuxProcessStat(pid)
	return alive && current == token
}

func linuxProcessStat(pid int) (string, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", false
	}
	value := string(data)
	end := strings.LastIndexByte(value, ')')
	if end < 0 {
		return "", false
	}
	fields := strings.Fields(value[end+1:])
	if len(fields) <= 19 || fields[0] == "Z" {
		return "", false
	}
	return fields[19], true
}
