package cliui

import (
	"bytes"
	"strings"
	"testing"
)

func TestRendererColorModes(t *testing.T) {
	var output bytes.Buffer
	renderer := New(&output, &output, Options{Color: ColorAlways, Width: 80})
	renderer.Println(renderer.Text(StyleStderr, "stderr"))
	if !strings.Contains(output.String(), "\x1b[31mstderr") {
		t.Fatalf("colored output = %q", output.String())
	}

	output.Reset()
	renderer = New(&output, &output, Options{Color: ColorNever, Width: 80})
	renderer.Println(renderer.Text(StyleStderr, "stderr"))
	if strings.Contains(output.String(), "\x1b[") {
		t.Fatalf("unexpected ANSI output = %q", output.String())
	}
}

func TestNoColorAndJSON(t *testing.T) {
	t.Setenv("NO_COLOR", "1")

	var output bytes.Buffer
	renderer := New(&output, &output, Options{Color: ColorAuto, Width: 80})
	renderer.Println(renderer.Text(StyleSuccess, "success"))
	if strings.Contains(output.String(), "\x1b[") {
		t.Fatalf("NO_COLOR should disable auto color: %q", output.String())
	}

	output.Reset()
	renderer = New(&output, &output, Options{Color: ColorAlways, Width: 80})
	renderer.Println(renderer.Text(StyleSuccess, "success"))
	if !strings.Contains(output.String(), "\x1b[32msuccess") {
		t.Fatalf("explicit always should override NO_COLOR: %q", output.String())
	}

	output.Reset()
	if err := renderer.JSON(map[string]string{"status": "ok"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "\x1b[") {
		t.Fatalf("JSON must not contain ANSI: %q", output.String())
	}

	output.Reset()
	renderer = New(&output, &output, Options{Color: ColorAlways, JSON: true, Width: 80})
	renderer.Errorf("error: %s", "failed")
	if strings.Contains(output.String(), "\x1b[") {
		t.Fatalf("JSON errors must not contain ANSI: %q", output.String())
	}
}

func TestRendererErrorStream(t *testing.T) {
	var output, errors bytes.Buffer
	renderer := New(&output, &errors, Options{Color: ColorAlways, Width: 80})
	renderer.Println("normal")
	renderer.Errorf("error: %s", "failed")
	if output.String() != "normal\n" {
		t.Fatalf("stdout = %q", output.String())
	}
	if !strings.Contains(errors.String(), "\x1b[31merror: failed") {
		t.Fatalf("stderr = %q", errors.String())
	}
}

func TestRendererUsesConfiguredLineEnding(t *testing.T) {
	var output bytes.Buffer
	renderer := New(&output, &output, Options{Color: ColorNever, Width: 80})
	renderer.SetLineEnding("\r\n")
	renderer.Printf("first\nsecond\r\n")
	renderer.Println("third")
	renderer.PrintStyled(StyleNone, "fourth\nfifth")

	if output.String() != "first\r\nsecond\r\nthird\r\nfourth\r\nfifth" {
		t.Fatalf("stdout = %q", output.String())
	}
}

func TestRendererTableTruncatesToWidth(t *testing.T) {
	var output bytes.Buffer
	renderer := New(&output, &output, Options{Color: ColorNever, Width: 32})
	renderer.Table([]string{"PROCESS", "STATE", "COMMAND"}, [][]Cell{{
		{Text: "demo/api"},
		{Text: "running"},
		{Text: "a-very-long-command-line-that-needs-truncation"},
	}})
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if len([]rune(line)) > 32 {
			t.Fatalf("line width = %d, line = %q", len([]rune(line)), line)
		}
	}
	if !strings.Contains(output.String(), "...") {
		t.Fatalf("table was not truncated: %q", output.String())
	}
}

func TestRendererTableUsesCompactSeparatorsOnNarrowTerminals(t *testing.T) {
	var output bytes.Buffer
	renderer := New(&output, &output, Options{Color: ColorNever, Width: 12})
	renderer.Table([]string{"A", "B", "C", "D"}, [][]Cell{{
		{Text: "one"}, {Text: "two"}, {Text: "three"}, {Text: "four"},
	}})
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if len([]rune(line)) > 12 {
			t.Fatalf("line width = %d, line = %q", len([]rune(line)), line)
		}
	}
}
