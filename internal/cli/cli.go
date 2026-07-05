// Package cli implements the pcs command-line interface: subcommand
// routing, usage output and the version report.
package cli

import (
	"fmt"
	"os"
)

// version is the PCS release. The Go port continues from the stored
// procedure implementation, which ended at 3.0.0. Overridable at build
// time with: -ldflags "-X .../internal/cli.version=x.y.z"
var version = "4.0.0-dev"

// command is one pcs subcommand. run receives the arguments after the
// subcommand name and returns the process exit code.
type command struct {
	name     string
	synopsis string
	run      func(args []string) int
}

// commands lists every subcommand in the order shown by usage.
func commands() []command {
	return []command{
		{"install", "create the pcs schema and discover already-partitioned tables", cmdInstall},
		{"add", "register a table for partition management", cmdAdd},
		{"run", "create and drop partitions for all registered tables", cmdRun},
		{"check", "verify partition coverage (monitoring-friendly exit codes)", cmdCheck},
		{"status", "show registered tables, states and recent log entries", cmdStatus},
		{"ping", "test connectivity; report server version and replica state", cmdPing},
		{"version", "print the pcs version", cmdVersion},
	}
}

// Run dispatches to a subcommand and returns the process exit code.
func Run(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return 2
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		usage(os.Stdout)
		return 0
	case "-version", "--version":
		return cmdVersion(nil)
	}
	for _, c := range commands() {
		if c.name == args[0] {
			return c.run(args[1:])
		}
	}
	fmt.Fprintf(os.Stderr, "pcs: unknown command %q\n\n", args[0])
	usage(os.Stderr)
	return 2
}

func usage(w *os.File) {
	fmt.Fprintf(w, "pcs %s - Partition Control System for MySQL 5.7-9.x\n\n", version)
	fmt.Fprintln(w, "Usage: pcs <command> [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, c := range commands() {
		fmt.Fprintf(w, "  %-10s %s\n", c.name, c.synopsis)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run 'pcs <command> -h' for command flags.")
}

func cmdVersion(_ []string) int {
	fmt.Printf("pcs version %s\n", version)
	return 0
}

// notImplemented is the placeholder body for subcommands whose port from
// the stored procedures has not landed yet (see CONVERSION_PLAN.md).
func notImplemented(name, task string) int {
	fmt.Fprintf(os.Stderr, "pcs %s: not implemented yet (CONVERSION_PLAN.md %s)\n", name, task)
	return 1
}
