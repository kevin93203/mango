// Package target parses the stable target grammar exposed by the Mango CLI.
//
// Keeping the syntax here prevents individual commands from quietly growing
// different interpretations of PROJECT/NAME and log targets. Runtime lookup
// (including numeric service IDs and run-prefix ambiguity) remains a daemon
// concern.
package target

import (
	"fmt"
	"strings"
)

type Kind string

const (
	Service  Kind = "service"
	Project  Kind = "project"
	Task     Kind = "task"
	Workflow Kind = "workflow"
	Schedule Kind = "schedule"
)

type Reference struct {
	Raw     string
	Kind    Kind
	Project string
	Name    string
	Node    string
}

func ParseProject(value string) (Reference, error) {
	parts, err := split(value, "project target", 1)
	if err != nil {
		return Reference{}, err
	}
	if !validProjectName(parts[0]) {
		return Reference{}, invalid(value, "PROJECT")
	}
	return Reference{Raw: value, Kind: Project, Project: parts[0], Name: parts[0]}, nil
}

func ParseService(value string) (Reference, error) {
	if isDigits(value) {
		return Reference{Raw: value, Kind: Service, Name: value}, nil
	}
	parts, err := split(value, "service target", 2)
	if err != nil || !validProjectName(parts[0]) || !validName(parts[1]) {
		return Reference{}, invalid(value, "PROJECT/SERVICE")
	}
	return Reference{Raw: value, Kind: Service, Project: parts[0], Name: parts[1]}, nil
}

func ParseTask(value string) (Reference, error) {
	return parseQualified(value, Task, "PROJECT/TASK")
}

func ParseWorkflow(value string) (Reference, error) {
	return parseQualified(value, Workflow, "PROJECT/WORKFLOW")
}

func ParseSchedule(value string) (Reference, error) {
	if project, err := ParseProject(value); err == nil {
		project.Kind = Schedule
		return project, nil
	}
	return parseQualified(value, Schedule, "PROJECT/SCHEDULE")
}

func ParseLog(value string) (Reference, error) {
	if isDigits(value) {
		return Reference{Raw: value, Kind: Service, Name: value}, nil
	}
	parts := strings.Split(value, "/")
	switch {
	case len(parts) == 2 && validProjectName(parts[0]) && validName(parts[1]):
		return Reference{Raw: value, Kind: Service, Project: parts[0], Name: parts[1]}, nil
	case len(parts) == 3 && parts[1] == "task" && validProjectName(parts[0]) && validName(parts[2]):
		return Reference{Raw: value, Kind: Task, Project: parts[0], Name: parts[2]}, nil
	case len(parts) == 4 && parts[1] == "workflow" && validProjectName(parts[0]) && validName(parts[2]) && validName(parts[3]):
		return Reference{Raw: value, Kind: Workflow, Project: parts[0], Name: parts[2], Node: parts[3]}, nil
	default:
		return Reference{}, invalid(value, "PROJECT/SERVICE, PROJECT/task/TASK, or PROJECT/workflow/WORKFLOW/NODE")
	}
}

func ParseRun(value string) (string, error) {
	if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "/\\ \t\r\n") {
		return "", invalid(value, "RUN_REF or unique RUN_REF prefix")
	}
	return value, nil
}

func ParseServiceOperation(value string) (Reference, error) {
	if isDigits(value) {
		return ParseService(value)
	}
	if !strings.Contains(value, "/") {
		project, err := ParseProject(value)
		if err != nil {
			return Reference{}, fmt.Errorf("invalid service operation target %q: expected PROJECT, PROJECT/SERVICE, or service ID", value)
		}
		project.Kind = Service
		return project, nil
	}
	return ParseService(value)
}

func parseQualified(value string, kind Kind, form string) (Reference, error) {
	parts, err := split(value, string(kind)+" target", 2)
	if err != nil || !validProjectName(parts[0]) || !validName(parts[1]) {
		return Reference{}, invalid(value, form)
	}
	return Reference{Raw: value, Kind: kind, Project: parts[0], Name: parts[1]}, nil
}

func split(value, label string, count int) ([]string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != count {
		return nil, fmt.Errorf("invalid %s %q", label, value)
	}
	return parts, nil
}

func invalid(value, form string) error {
	return fmt.Errorf("invalid target %q; expected %s", value, form)
}

func validProjectName(value string) bool {
	if value == "" || (value[0] < 'A' || value[0] > 'Z') && (value[0] < 'a' || value[0] > 'z') {
		return false
	}
	return validName(value)
}

func validName(value string) bool {
	if value == "" || (value[0] < 'A' || value[0] > 'Z') && (value[0] < 'a' || value[0] > 'z') && (value[0] < '0' || value[0] > '9') {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !isNameChar(value[index]) {
			return false
		}
	}
	return true
}

func isNameChar(char byte) bool {
	return (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-'
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range []byte(value) {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
