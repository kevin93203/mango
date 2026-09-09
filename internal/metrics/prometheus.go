package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type metricValue struct {
	Name   string
	Labels map[string]string
	Value  float64
}

type Registry struct {
	mu     sync.Mutex
	values map[string]metricValue
}

func NewRegistry() *Registry { return &Registry{values: map[string]metricValue{}} }

func (r *Registry) Add(name string, value float64, labels map[string]string) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := metricKey(name, labels)
	item := r.values[key]
	item.Name, item.Labels = name, cloneLabels(labels)
	item.Value += value
	r.values[key] = item
}

func (r *Registry) Set(name string, value float64, labels map[string]string) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := metricKey(name, labels)
	r.values[key] = metricValue{Name: name, Labels: cloneLabels(labels), Value: value}
}

func (r *Registry) Prometheus() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	values := make([]metricValue, 0, len(r.values))
	for _, item := range r.values {
		values = append(values, item)
	}
	r.mu.Unlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].Name != values[j].Name {
			return values[i].Name < values[j].Name
		}
		return metricKey(values[i].Name, values[i].Labels) < metricKey(values[j].Name, values[j].Labels)
	})
	var output strings.Builder
	for _, item := range values {
		output.WriteString(item.Name)
		if len(item.Labels) > 0 {
			keys := make([]string, 0, len(item.Labels))
			for key := range item.Labels {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			output.WriteByte('{')
			for index, key := range keys {
				if index > 0 {
					output.WriteByte(',')
				}
				output.WriteString(key)
				output.WriteString("=\"")
				output.WriteString(strings.ReplaceAll(strings.ReplaceAll(item.Labels[key], "\\", "\\\\"), "\"", "\\\""))
				output.WriteString("\"")
			}
			output.WriteByte('}')
		}
		output.WriteByte(' ')
		output.WriteString(strconv.FormatFloat(item.Value, 'f', -1, 64))
		output.WriteByte('\n')
	}
	return output.String()
}

func metricKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	key := name
	for _, label := range keys {
		key += "\x00" + label + "=" + labels[label]
	}
	return key
}

func cloneLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	result := make(map[string]string, len(labels))
	for key, value := range labels {
		result[key] = value
	}
	return result
}

func (r *Registry) String() string { return fmt.Sprintf("%s", r.Prometheus()) }
