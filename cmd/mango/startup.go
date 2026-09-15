package main

import (
	"os"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/startup"
	"golang.org/x/term"
)

func termIsInteractive() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

func startupInstallCommand() error {
	executable, err := resolveDaemonExecutable()
	if err != nil {
		return err
	}
	if err := startup.Install(executable); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(map[string]string{"status": "installed", "executable": executable})
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleSuccess, "Startup integration installed"))
	return nil
}

func startupUninstallCommand() error {
	if err := startup.Uninstall(); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(map[string]string{"status": "uninstalled"})
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleSuccess, "Startup integration uninstalled"))
	return nil
}

func startupStatusCommand() error {
	status, err := startup.GetStatus()
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(status)
	}
	printStartupStatus(status)
	return nil
}
