package metrics

import (
	"maps"
	"sort"
	"strconv"
	"strings"
)

type metricValue struct {
	Name   string
	Labels map[string]string
	Value  float64
}

func (c *Collector) Add(name string, value float64, labels map[string]string) {
	if c == nil || name == "" {
		return
	}
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	if c.values == nil {
		c.values = map[string]metricValue{}
	}
	key := metricKey(name, labels)
	item := c.values[key]
	item.Name, item.Labels = name, cloneLabels(labels)
	item.Value += value
	c.values[key] = item
}

func (c *Collector) Set(name string, value float64, labels map[string]string) {
	if c == nil || name == "" {
		return
	}
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	if c.values == nil {
		c.values = map[string]metricValue{}
	}
	key := metricKey(name, labels)
	c.values[key] = metricValue{Name: name, Labels: cloneLabels(labels), Value: value}
}

func (c *Collector) Prometheus() string {
	if c == nil {
		return ""
	}
	c.metricsMu.Lock()
	values := make([]metricValue, 0, len(c.values))
	for _, item := range c.values {
		values = append(values, item)
	}
	c.metricsMu.Unlock()
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
	return maps.Clone(labels)
}
