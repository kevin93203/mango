package tui

import (
	"os"

	"github.com/kevin93203/mango/internal/cliui"
	"golang.org/x/term"
)

type historyInteractiveModel interface {
	historyHandleKey(int) (bool, error)
	historyRender(*cliui.Renderer)
	historyError() string
	historySetError(string)
}

func runHistoryInteractive(output *cliui.Renderer, model historyInteractiveModel) error {
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer term.Restore(int(os.Stdin.Fd()), state)
	output.SetLineEnding("\r\n")
	defer output.SetLineEnding("\n")
	output.Printf("\x1b[?1049h\x1b[?25l")
	defer func() {
		output.Printf("\x1b[?1049l\x1b[?25h\x1b[0m\n")
	}()

	input := make(chan byte, 8)
	go readInput(input)
	draw := func() {
		output.Printf("\x1b[H\x1b[2J")
		model.historyRender(output)
		if message := model.historyError(); message != "" {
			output.Printf("\n%s\n", output.ErrorText("error: "+message))
		}
	}
	draw()
	for {
		key := readWorkflowHistoryKey(input)
		quit, err := model.historyHandleKey(key)
		if err != nil {
			model.historySetError(err.Error())
		}
		if quit {
			return nil
		}
		draw()
	}
}
