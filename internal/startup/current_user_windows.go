package startup

import (
	"fmt"

	"golang.org/x/sys/windows"
)

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
