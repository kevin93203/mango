package scheduler

func counterKey(kind, project, target string) string {
	return kind + "|" + project + "|" + target
}

func counterKeys(record Record) []string {
	keys := make([]string, 0, len(record.Tasks)+3)
	if record.TargetType != "" && record.Target != "" {
		keys = append(keys, counterKey(record.TargetType, record.Project, record.Target))
	}
	if record.Trigger.Type != "" && record.Trigger.Name != "" {
		keys = append(keys, counterKey("trigger:"+record.Trigger.Type, record.Project, record.Trigger.Name))
	}
	if record.TargetType == "workflow" {
		for _, task := range record.Tasks {
			if task.Task != "" {
				keys = append(keys, counterKey("task", record.Project, task.Task))
			}
		}
	}
	return keys
}

// CounterKeys exposes the stable counter dimensions to history repository
// implementations without exposing the counter storage format.
func CounterKeys(record Record) []string {
	return counterKeys(record)
}

func copyCounts(values map[string]uint64) map[string]uint64 {
	result := make(map[string]uint64, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
