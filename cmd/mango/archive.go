package main

import (
	"context"
	"fmt"
	"strings"

	projectarchive "github.com/kevin93203/mango/internal/archive"
	"github.com/spf13/cobra"
)

func (a *cliApp) exportCmd() *cobra.Command {
	var output string
	var force bool
	cmd := a.leafCmdWithContext("export [PROJECT...]", "Export project data", cobra.ArbitraryArgs, func(ctx context.Context, args []string) error {
		result, err := projectarchive.Export(ctx, a.layout, projectarchive.ExportOptions{Projects: args, Output: output, Force: force})
		if err != nil {
			return err
		}
		return printArchiveResult(result)
	})
	cmd.Flags().StringVarP(&output, "output", "o", "", "output archive path")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite an existing output archive")
	a.addJSONFlag(cmd)
	cmd.GroupID = groupAdvanced
	return cmd
}

func (a *cliApp) importCmd() *cobra.Command {
	var configDir string
	var rename []string
	var replace, yes, dryRun bool
	cmd := a.leafCmdWithContext("import ARCHIVE [PROJECT...]", "Import project data", cobra.MinimumNArgs(1), func(ctx context.Context, args []string) error {
		if yes && !replace {
			return fmt.Errorf("import --yes requires --replace")
		}
		mapping, err := parseArchiveRenames(rename)
		if err != nil {
			return err
		}
		result, err := projectarchive.Import(ctx, a.layout, projectarchive.ImportOptions{
			Archive: args[0], Projects: args[1:], ConfigDir: configDir, Rename: mapping,
			Replace: replace, Yes: yes, DryRun: dryRun,
		})
		if err != nil {
			return err
		}
		return printArchiveResult(result)
	})
	cmd.Flags().StringVar(&configDir, "config-dir", ".", "directory for imported project configurations")
	cmd.Flags().StringArrayVar(&rename, "rename", nil, "rename a source project as OLD=NEW; repeatable")
	cmd.Flags().BoolVar(&replace, "replace", false, "replace existing destination project data")
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm replacement")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate and preview without writing")
	a.addJSONFlag(cmd)
	cmd.GroupID = groupAdvanced
	return cmd
}

func parseArchiveRenames(values []string) (map[string]string, error) {
	result := make(map[string]string, len(values))
	for _, value := range values {
		oldName, newName, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(oldName) == "" || strings.TrimSpace(newName) == "" {
			return nil, fmt.Errorf("--rename expects OLD=NEW, got %q", value)
		}
		oldName = strings.TrimSpace(oldName)
		newName = strings.TrimSpace(newName)
		if _, exists := result[oldName]; exists {
			return nil, fmt.Errorf("--rename source %q was specified more than once", oldName)
		}
		result[oldName] = newName
	}
	return result, nil
}

func printArchiveResult(result projectarchive.Result) error {
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	if result.DryRun {
		cliOutput.Println("Dry run: no destination files were changed.")
	}
	verb := "imported"
	if result.Operation == "export" {
		verb = "exported"
	}
	for _, project := range result.Projects {
		if project.Source == project.Project {
			cliOutput.Println(fmt.Sprintf("%s %s", verb, project.Project))
		} else {
			cliOutput.Println(fmt.Sprintf("%s %s as %s", verb, project.Source, project.Project))
		}
	}
	if result.Archive != "" {
		cliOutput.Println(fmt.Sprintf("archive: %s", result.Archive))
	}
	return nil
}
