package cliui

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/term"
)

type ColorMode string

const (
	ColorAuto   ColorMode = "auto"
	ColorAlways ColorMode = "always"
	ColorNever  ColorMode = "never"
)

type Options struct {
	Color ColorMode
	JSON  bool
	Width int
}

type Style int

const (
	StyleNone Style = iota
	StyleSuccess
	StyleWarning
	StyleError
	StyleHeader
	StyleMuted
	StyleStdout
	StyleStderr
)

type Align int

const (
	AlignLeft Align = iota
	AlignRight
)

type Cell struct {
	Text  string
	Style Style
	Align Align
}

type Renderer struct {
	out      io.Writer
	errOut   io.Writer
	width    int
	outColor bool
	errColor bool
	lineEnd  string
}

func ParseOptions(args []string) (Options, []string, error) {
	options := Options{Color: ColorAuto}
	remaining := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json":
			options.JSON = true
		case arg == "--color":
			if i+1 >= len(args) {
				return Options{}, nil, errors.New("--color requires auto, always, or never")
			}
			i++
			mode, err := parseColorMode(args[i])
			if err != nil {
				return Options{}, nil, err
			}
			options.Color = mode
		case strings.HasPrefix(arg, "--color="):
			mode, err := parseColorMode(strings.TrimPrefix(arg, "--color="))
			if err != nil {
				return Options{}, nil, err
			}
			options.Color = mode
		case arg == "--":
			remaining = append(remaining, args[i+1:]...)
			return options, remaining, nil
		default:
			remaining = append(remaining, arg)
		}
	}
	return options, remaining, nil
}

func parseColorMode(value string) (ColorMode, error) {
	mode := ColorMode(strings.ToLower(value))
	switch mode {
	case ColorAuto, ColorAlways, ColorNever:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid --color value %q; expected auto, always, or never", value)
	}
}

func New(out, errOut io.Writer, options Options) *Renderer {
	if options.Color == "" {
		options.Color = ColorAuto
	}
	noColor := options.JSON
	if _, ok := os.LookupEnv("NO_COLOR"); ok && options.Color == ColorAuto {
		noColor = true
	}
	outColor := !noColor && (options.Color == ColorAlways || (options.Color == ColorAuto && isTerminal(out)))
	errColor := !noColor && (options.Color == ColorAlways || (options.Color == ColorAuto && isTerminal(errOut)))
	if outColor && !enableTerminalColors(out) {
		outColor = false
	}
	if errColor && !enableTerminalColors(errOut) {
		errColor = false
	}
	width := options.Width
	if width <= 0 {
		width = terminalWidth(out)
	}
	return &Renderer{out: out, errOut: errOut, width: width, outColor: outColor, errColor: errColor, lineEnd: "\n"}
}

func (r *Renderer) Printf(format string, args ...interface{}) {
	r.writeOut(fmt.Sprintf(format, args...))
}

func (r *Renderer) Println(args ...interface{}) {
	r.writeOut(fmt.Sprintln(args...))
}

func (r *Renderer) Errorf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintln(r.errOut, r.ErrorText(message))
}

func (r *Renderer) Errorln(args ...interface{}) {
	message := strings.TrimSuffix(fmt.Sprintln(args...), "\n")
	_, _ = fmt.Fprintln(r.errOut, r.ErrorText(message))
}

func (r *Renderer) JSON(value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	r.Println(string(data))
	return nil
}

func (r *Renderer) Text(style Style, text string) string {
	return r.paint(style, text, r.outColor)
}

func (r *Renderer) ErrorText(text string) string {
	return r.paint(StyleError, text, r.errColor)
}

func (r *Renderer) PrintStyled(style Style, text string) {
	r.writeOut(r.Text(style, text))
}

// SetLineEnding controls line endings written to stdout. Interactive programs
// should use CRLF while the terminal is in raw mode because raw mode disables
// the terminal's usual LF-to-CRLF output conversion on Unix.
func (r *Renderer) SetLineEnding(lineEnd string) {
	if lineEnd == "" {
		lineEnd = "\n"
	}
	r.lineEnd = lineEnd
}

func (r *Renderer) writeOut(text string) {
	if r.lineEnd != "\n" {
		text = strings.ReplaceAll(text, "\r\n", "\n")
		text = strings.ReplaceAll(text, "\n", r.lineEnd)
	}
	_, _ = fmt.Fprint(r.out, text)
}

func (r *Renderer) Width() int {
	return r.width
}

func (r *Renderer) Table(headers []string, rows [][]Cell) {
	if len(headers) == 0 {
		return
	}
	widths := make([]int, len(headers))
	for i, header := range headers {
		widths[i] = visibleWidth(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && visibleWidth(cell.Text) > widths[i] {
				widths[i] = visibleWidth(cell.Text)
			}
		}
	}
	for i := range widths {
		if widths[i] < 1 {
			widths[i] = 1
		}
	}
	columnSeparator := " | "
	ruleSeparator := "-+-"
	minimumWidths := make([]int, len(widths))
	for i := range minimumWidths {
		minimumWidths[i] = 1
	}
	if r.width < tableWidth(minimumWidths, len(columnSeparator)) {
		columnSeparator = "|"
		ruleSeparator = "+"
	}
	if r.width < tableWidth(minimumWidths, len(columnSeparator)) {
		columnSeparator = ""
		ruleSeparator = ""
	}
	widths = fitWidths(widths, r.width, len(columnSeparator))

	r.printTableRow(widths, columnSeparator, func(i int) Cell {
		return Cell{Text: headers[i], Style: StyleHeader}
	})
	separator := make([]string, len(widths))
	for i, width := range widths {
		separator[i] = strings.Repeat("-", width)
	}
	r.Println(r.Text(StyleMuted, strings.Join(separator, ruleSeparator)))
	for _, row := range rows {
		r.printTableRow(widths, columnSeparator, func(i int) Cell {
			if i >= len(row) {
				return Cell{}
			}
			return row[i]
		})
	}
}

func (r *Renderer) KeyValues(rows [][]Cell) {
	if len(rows) == 0 {
		return
	}
	formatted := make([][]Cell, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			formatted = append(formatted, row)
			continue
		}
		row[0].Style = StyleHeader
		formatted = append(formatted, row)
	}
	r.Table([]string{"FIELD", "VALUE"}, formatted)
}

func (r *Renderer) printTableRow(widths []int, separator string, cellAt func(int) Cell) {
	parts := make([]string, len(widths))
	for i, width := range widths {
		cell := cellAt(i)
		text := truncate(cell.Text, width)
		padding := width - visibleWidth(text)
		if padding < 0 {
			padding = 0
		}
		if cell.Align == AlignRight {
			text = strings.Repeat(" ", padding) + text
		} else {
			text += strings.Repeat(" ", padding)
		}
		parts[i] = r.Text(cell.Style, text)
	}
	r.Println(strings.Join(parts, separator))
}

func (r *Renderer) StateText(state string) string {
	return r.Text(StateStyle(state), state)
}

func StateStyle(state string) Style {
	switch strings.ToLower(state) {
	case "running", "ok", "success", "installed":
		return StyleSuccess
	case "failed", "crash_loop", "error":
		return StyleError
	case "starting", "stopping", "stopped", "exited", "backing_off", "disabled", "degraded", "unknown":
		return StyleWarning
	default:
		return StyleNone
	}
}

func FormatBytes(value uint64) string {
	if value == 0 {
		return "-"
	}
	units := []string{"B", "KiB", "MiB", "GiB"}
	n := float64(value)
	index := 0
	for n >= 1024 && index < len(units)-1 {
		n /= 1024
		index++
	}
	return fmt.Sprintf("%.1f%s", n, units[index])
}

func FormatDuration(seconds float64) string {
	if seconds <= 0 {
		return "-"
	}
	d := time.Duration(seconds * float64(time.Second))
	parts := make([]string, 0, 3)
	if hours := d / time.Hour; hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
		d %= time.Hour
	}
	if minutes := d / time.Minute; minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
		d %= time.Minute
	}
	parts = append(parts, fmt.Sprintf("%ds", d/time.Second))
	return strings.Join(parts, " ")
}

func (r *Renderer) paint(style Style, text string, enabled bool) string {
	if !enabled || style == StyleNone || text == "" {
		return text
	}
	code := map[Style]string{
		StyleSuccess: "32",
		StyleWarning: "33",
		StyleError:   "31",
		StyleHeader:  "36",
		StyleMuted:   "90",
		StyleStdout:  "37",
		StyleStderr:  "31",
	}[style]
	if code == "" {
		return text
	}
	return "\x1b[" + code + "m" + text + "\x1b[0m"
}

func isTerminal(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	return ok && term.IsTerminal(int(file.Fd()))
}

func terminalWidth(writer io.Writer) int {
	file, ok := writer.(*os.File)
	if ok && term.IsTerminal(int(file.Fd())) {
		if width, _, err := term.GetSize(int(file.Fd())); err == nil && width > 0 {
			return width
		}
	}
	return 120
}

func fitWidths(widths []int, target, separatorWidth int) []int {
	result := append([]int(nil), widths...)
	if target <= 0 {
		return result
	}
	minimum := len(result) + (len(result)-1)*separatorWidth
	if target < minimum {
		target = minimum
	}
	for tableWidth(result, separatorWidth) > target {
		index := -1
		for i, width := range result {
			if width > 1 && (index == -1 || width > result[index]) {
				index = i
			}
		}
		if index == -1 {
			break
		}
		result[index]--
	}
	return result
}

func tableWidth(widths []int, separatorWidth int) int {
	width := separatorWidth * (len(widths) - 1)
	for _, item := range widths {
		width += item
	}
	return width
}

func truncate(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if visibleWidth(value) <= width {
		return value
	}
	runes := []rune(value)
	if width <= 3 {
		return string(runes[:min(len(runes), width)])
	}
	return string(runes[:min(len(runes), width-3)]) + "..."
}

func visibleWidth(value string) int {
	value = stripANSI(value)
	return utf8.RuneCountInString(value)
}

func stripANSI(value string) string {
	var builder strings.Builder
	for i := 0; i < len(value); {
		if value[i] != '\x1b' || i+1 >= len(value) || value[i+1] != '[' {
			builder.WriteByte(value[i])
			i++
			continue
		}
		i += 2
		for i < len(value) {
			b := value[i]
			i++
			if b >= 0x40 && b <= 0x7e {
				break
			}
		}
	}
	return builder.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
