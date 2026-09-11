package startup

import (
	"fmt"
	"os"
	"os/user"

	"golang.org/x/sys/windows"
)

type startupUser struct {
	Name string
	UID  string
	GID  string
	Home string
}

func currentUser() (startupUser, error) {
	account, err := user.Current()
	if err != nil {
		return startupUser{}, fmt.Errorf("resolve current user: %w", err)
	}
	sid, err := currentUserID()
	if err != nil {
		return startupUser{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return startupUser{}, fmt.Errorf("resolve current user home: %w", err)
	}
	return startupUser{Name: account.Username, UID: sid, GID: account.Gid, Home: home}, nil
}

func currentUserID() (string, error) {
	account, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", fmt.Errorf("resolve current user SID: %w", err)
	}
	if account.User.Sid == nil {
		return "", fmt.Errorf("resolve current user SID: empty SID")
	}
	return account.User.Sid.String(), nil
}
