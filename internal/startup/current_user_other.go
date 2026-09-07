//go:build !windows

package startup

import (
	"fmt"
	"os/user"
)

func currentUserID() (string, error) {
	account, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("resolve current user ID: %w", err)
	}
	if account.Uid == "" {
		return "", fmt.Errorf("resolve current user ID: empty user ID")
	}
	return account.Uid, nil
}
