package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
)

func logsCommandWithContext(ctx context.Context, targets []string, options logsOptions) error {
	return logsCommandWithCallerAndContext(ctx, targets, options, logsCall)
}

func monitorLogs(output *cliui.Renderer, key string, input <-chan byte) error {
	resolved, err := resolveLogTargets([]string{key}, logsCall)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-input:
			cancel()
		case <-ctx.Done():
		}
	}()

	output.Printf("\x1b[2J\x1b[H")
	output.Printf("%s %s\n\n", output.Text(cliui.StyleHeader, fmt.Sprintf("%s logs", resolved[0].canonical)), output.Text(cliui.StyleMuted, "(press any key to return)"))
	return followLogsTargetsWithContext(ctx, resolved, "all", 15, logsCall, newLogWriterFor(output))
}

type logsOptions struct {
	stream string
	tail   int
	follow bool
}

type logsCaller func(context.Context, string, interface{}) (ipc.Response, error)

type resolvedLogTarget struct {
	input     string
	canonical string
}

type logStream struct {
	targetIndex int
	target      string
	stream      string
	offset      int64
}

type logEvent struct {
	stream *logStream
	data   string
	offset int64
}

func logsCommandWithCaller(targets []string, options logsOptions, caller logsCaller) error {
	return logsCommandWithCallerAndContext(cliCommandContext, targets, options, caller)
}

func logsCommandWithCallerAndContext(ctx context.Context, targets []string, options logsOptions, caller logsCaller) error {
	if len(targets) == 0 {
		return errors.New("logs requires PROJECT/SERVICE, PROJECT/task/TASK, PROJECT/workflow/WORKFLOW/NODE, or ID")
	}
	if options.tail < 0 {
		return errors.New("logs tail must be non-negative")
	}
	if options.stream != "" && options.stream != "stdout" && options.stream != "stderr" && options.stream != "all" {
		return fmt.Errorf("logs stream must be stdout, stderr, or all")
	}
	resolved, err := resolveLogTargetsWithContext(ctx, targets, caller)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	if options.follow {
		if jsonOutput {
			return errors.New("--json is not supported with logs --follow; use logs without --follow")
		}
		return followLogsTargetsWithContext(ctx, resolved, options.stream, options.tail, caller, newLogWriter())
	}
	if jsonOutput {
		return readLogsTargetsJSONWithContext(ctx, resolved, options.stream, options.tail, caller)
	}
	return readLogsTargetsWithContext(ctx, resolved, options.stream, options.tail, caller)
}

type logJSONEntry struct {
	Target string `json:"target"`
	Stream string `json:"stream"`
	Data   string `json:"data"`
}

func readLogsTargetsJSONWithContext(ctx context.Context, targets []resolvedLogTarget, stream string, tail int, caller logsCaller) error {
	streams := logStreams(targets, stream)
	events, errs := readLogEvents(streams, func(item *logStream) (ipc.Response, error) {
		return caller(ctx, "logs.read", struct {
			Key    string
			Stream string
			Tail   int
		}{item.target, item.stream, tail})
	}, func(response ipc.Response) (string, int64, error) {
		var data struct {
			Data string `json:"data"`
		}
		if err := decodeData(response.Data, &data); err != nil {
			return "", 0, err
		}
		return data.Data, 0, nil
	})
	if err := joinLogErrors(errs); err != nil {
		return err
	}
	streamOrder := map[string]int{"stdout": 0, "stderr": 1}
	sort.SliceStable(events, func(left, right int) bool {
		if events[left].stream.targetIndex != events[right].stream.targetIndex {
			return events[left].stream.targetIndex < events[right].stream.targetIndex
		}
		return streamOrder[events[left].stream.stream] < streamOrder[events[right].stream.stream]
	})
	entries := make([]logJSONEntry, 0, len(events))
	for _, event := range events {
		entries = append(entries, logJSONEntry{Target: event.stream.target, Stream: event.stream.stream, Data: event.data})
	}
	return cliOutput.JSON(entries)
}

func resolveLogTargets(targets []string, caller logsCaller) ([]resolvedLogTarget, error) {
	return resolveLogTargetsWithContext(context.Background(), targets, caller)
}

func resolveLogTargetsWithContext(ctx context.Context, targets []string, caller logsCaller) ([]resolvedLogTarget, error) {
	resolved := make([]resolvedLogTarget, len(targets))
	errs := make([]error, len(targets))
	for i, target := range targets {
		response, err := caller(ctx, "logs.resolve", struct{ Key string }{target})
		if err != nil {
			errs[i] = fmt.Errorf("logs target %q: %w", target, err)
			continue
		}
		var data struct {
			Key string `json:"key"`
		}
		if err := decodeData(response.Data, &data); err != nil {
			errs[i] = fmt.Errorf("logs target %q: decode response: %w", target, err)
			continue
		}
		if data.Key == "" {
			errs[i] = fmt.Errorf("logs target %q: resolver returned an empty canonical target", target)
			continue
		}
		resolved[i] = resolvedLogTarget{input: target, canonical: data.Key}
	}
	var joined error
	for _, err := range errs {
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	if joined != nil {
		return nil, joined
	}
	return resolved, nil
}

func logStreams(targets []resolvedLogTarget, stream string) []*logStream {
	streams := []string{stream}
	if stream == "all" {
		streams = []string{"stdout", "stderr"}
	}
	result := make([]*logStream, 0, len(targets)*len(streams))
	for targetIndex, target := range targets {
		for _, item := range streams {
			result = append(result, &logStream{targetIndex: targetIndex, target: target.canonical, stream: item})
		}
	}
	return result
}

func readLogsTargetsWithContext(ctx context.Context, targets []resolvedLogTarget, stream string, tail int, caller logsCaller) error {
	streams := logStreams(targets, stream)
	events, errs := readLogEvents(streams, func(item *logStream) (ipc.Response, error) {
		return caller(ctx, "logs.read", struct {
			Key    string
			Stream string
			Tail   int
		}{targets[item.targetIndex].canonical, item.stream, tail})
	}, func(response ipc.Response) (string, int64, error) {
		var data struct {
			Data string `json:"data"`
		}
		if err := decodeData(response.Data, &data); err != nil {
			return "", 0, err
		}
		return data.Data, 0, nil
	})
	if err := joinLogErrors(errs); err != nil {
		return err
	}

	writer := newLogWriter()
	buffers := make(map[*logStream]*logLineBuffer, len(streams))
	for _, item := range streams {
		buffers[item] = &logLineBuffer{}
	}
	for _, event := range events {
		buffers[event.stream].write(event.data, func(line string) {
			writer.write(event.stream.target, event.stream.stream, line)
		})
		// Each one-shot read is the final chunk for this stream. Flush here so
		// an unterminated line keeps the response arrival order as well.
		buffers[event.stream].flush(func(line string) {
			writer.write(event.stream.target, event.stream.stream, line)
		})
	}
	return nil
}

func readLogEvents(streams []*logStream, read func(*logStream) (ipc.Response, error), decode func(ipc.Response) (string, int64, error)) ([]logEvent, []error) {
	events := make(chan logEvent, len(streams))
	errs := make(chan error, len(streams))
	var wait sync.WaitGroup
	for _, item := range streams {
		item := item
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := read(item)
			if err != nil {
				errs <- fmt.Errorf("logs target %s %s stream: %w", item.target, item.stream, err)
				return
			}
			data, offset, err := decode(response)
			if err != nil {
				errs <- fmt.Errorf("logs target %s %s stream: decode response: %w", item.target, item.stream, err)
				return
			}
			events <- logEvent{stream: item, data: data, offset: offset}
		}()
	}
	wait.Wait()
	close(events)
	close(errs)
	result := make([]logEvent, 0, len(streams))
	for event := range events {
		result = append(result, event)
	}
	resultErrs := make([]error, 0)
	for err := range errs {
		resultErrs = append(resultErrs, err)
	}
	return result, resultErrs
}

func joinLogErrors(errs []error) error {
	var joined error
	for _, err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func clearLogsCommand(target string) error {
	response, err := call("logs.clear", struct{ Key string }{target})
	if err != nil {
		return err
	}
	var result map[string]string
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Logs cleared for %s", result["key"])))
	return nil
}

func followLogsTargetsWithContext(ctx context.Context, targets []resolvedLogTarget, stream string, tail int, caller logsCaller, writer *logWriter) error {
	streams := logStreams(targets, stream)
	initialEvents, errs := readLogEvents(streams, func(item *logStream) (ipc.Response, error) {
		return caller(ctx, "logs.read", struct {
			Key           string
			Stream        string
			Tail          int
			IncludeOffset bool
		}{item.target, item.stream, tail, true})
	}, func(response ipc.Response) (string, int64, error) {
		var data followLogResponse
		if err := decodeData(response.Data, &data); err != nil {
			return "", 0, err
		}
		return data.Data, data.NextOffset, nil
	})
	if err := joinLogErrors(errs); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	buffers := make(map[*logStream]*logLineBuffer, len(streams))
	for _, item := range streams {
		buffers[item] = &logLineBuffer{}
	}
	for _, event := range initialEvents {
		event.stream.offset = event.offset
		buffers[event.stream].write(event.data, func(line string) {
			writer.write(event.stream.target, event.stream.stream, line)
		})
	}

	for {
		if ctx.Err() != nil {
			flushLogBuffers(streams, buffers, writer)
			return nil
		}
		errs := readLogEventsLive(streams, func(item *logStream) (ipc.Response, error) {
			return caller(ctx, "logs.read", struct {
				Key      string
				Stream   string
				Offset   int64
				MaxBytes int
			}{item.target, item.stream, item.offset, 64 << 10})
		}, func(response ipc.Response) (string, int64, error) {
			var data followLogResponse
			if err := decodeData(response.Data, &data); err != nil {
				return "", 0, err
			}
			return data.Data, data.NextOffset, nil
		}, func(event logEvent) {
			event.stream.offset = event.offset
			buffers[event.stream].write(event.data, func(line string) {
				writer.write(event.stream.target, event.stream.stream, line)
			})
		})
		if err := joinLogErrors(errs); err != nil {
			flushLogBuffers(streams, buffers, writer)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			flushLogBuffers(streams, buffers, writer)
			return nil
		case <-timer.C:
		}
	}
}

func flushLogBuffers(streams []*logStream, buffers map[*logStream]*logLineBuffer, writer *logWriter) {
	for _, item := range streams {
		buffers[item].flush(func(line string) {
			writer.write(item.target, item.stream, line)
		})
	}
}

type logEventResult struct {
	event logEvent
	err   error
}

func readLogEventsLive(streams []*logStream, read func(*logStream) (ipc.Response, error), decode func(ipc.Response) (string, int64, error), emit func(logEvent)) []error {
	results := make(chan logEventResult, len(streams))
	for _, item := range streams {
		item := item
		go func() {
			response, err := read(item)
			if err != nil {
				results <- logEventResult{err: fmt.Errorf("logs target %s %s stream: %w", item.target, item.stream, err)}
				return
			}
			data, offset, err := decode(response)
			if err != nil {
				results <- logEventResult{err: fmt.Errorf("logs target %s %s stream: decode response: %w", item.target, item.stream, err)}
				return
			}
			results <- logEventResult{event: logEvent{stream: item, data: data, offset: offset}}
		}()
	}
	errs := make([]error, 0)
	for range streams {
		result := <-results
		if result.err != nil {
			errs = append(errs, result.err)
			continue
		}
		emit(result.event)
	}
	return errs
}

type logWriter struct {
	mu     sync.Mutex
	output *cliui.Renderer
}

func newLogWriter() *logWriter { return newLogWriterFor(cliOutput) }

func newLogWriterFor(output *cliui.Renderer) *logWriter { return &logWriter{output: output} }

func (w *logWriter) write(target, stream, line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	output := w.output
	style := cliui.StyleLogStdout
	if stream == "stderr" {
		style = cliui.StyleStderr
	}
	prefix := output.Text(style, "｜"+target+"｜")
	output.Printf("%s %s\n", prefix, line)
}

type logLineBuffer struct {
	data string
}

func (b *logLineBuffer) write(data string, emit func(string)) {
	if data == "" {
		return
	}
	b.data += data
	for {
		index := strings.IndexByte(b.data, '\n')
		if index < 0 {
			return
		}
		line := strings.TrimSuffix(b.data[:index], "\r")
		emit(line)
		b.data = b.data[index+1:]
	}
}

func (b *logLineBuffer) flush(emit func(string)) {
	if b.data == "" {
		return
	}
	emit(strings.TrimSuffix(b.data, "\r"))
	b.data = ""
}

type followLogResponse struct {
	Data       string `json:"data"`
	NextOffset int64  `json:"next_offset"`
}
