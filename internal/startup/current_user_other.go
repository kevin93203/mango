//go:build !windows

package startup

import (
	"fmt"
	"os"
	"os/user"
)

type startupUser struct {
	Name string
	UID  string
	GID  string
	Home string
}

func currentUser() (startupUser, error) {
	account, err := currentUserAccount()
	if err != nil {
		return startupUser{}, err
	}
	return startupUser{
		Name: account.Username,
		UID:  account.Uid,
		GID:  account.Gid,
		Home: account.HomeDir,
	}, nil
}

func currentUserID() (string, error) {
	account, err := currentUserAccount()
	if err != nil {
		return "", fmt.Errorf("resolve current user ID: %w", err)
	}
	if account.Uid == "" {
		return "", fmt.Errorf("resolve current user ID: empty user ID")
	}
	return account.Uid, nil
}

func currentUserAccount() (*user.User, error) {
	if os.Geteuid() == 0 {
		if uid := os.Getenv("SUDO_UID"); uid != "" {
			account, err := user.LookupId(uid)
			if err != nil {
				return nil, fmt.Errorf("resolve sudo user %s: %w", uid, err)
			}
			return account, nil
		}
	}
	account, err := user.Current()
	if err != nil {
		return nil, err
	}
	return account, nil
}
