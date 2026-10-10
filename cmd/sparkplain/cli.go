package main

import (
	"context"
	"io"
	"strings"

	"github.com/charmbracelet/fang"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// The help page. Fang draws it (the same look as aws_explorer --help):
// long text, then usage, examples and flags, styled only on a terminal.
const (
	cliUse  = "sparkplain --app-id <id> [--eventlog <path>] [--profile <p> --cluster-id <id> | --from <folder>]"
	cliLong = `sparkplain turns one Spark application's event log and its YARN, step and node logs into three files: <app-id>-report.html, <app-id>-report.json and <app-id>-explorer.html.

Online runs read the cluster's metadata and logs from AWS with read-only calls. Offline runs read a local copy of the logs. The event log is optional; without it, some sections are marked "needs event log".

Defaults come from ~/.config/sparkplain/config.yaml when it exists. Run "sparkplain --init-config" to write a starter file. Each flag takes one dash or two: -app-id and --app-id are the same.

Exit codes: 0 complete, 2 fatal, 3 partial (a source missing or unreadable), 130 interrupted.`
	cliExample = `# Event log only, from a History Server download
sparkplain --app-id application_1700000000000_0042 --eventlog ./application_1700000000000_0042.zip

# Online: the cluster's metadata and logs from AWS
sparkplain --app-id application_1700000000000_0042 --profile prod --cluster-id j-ABC123

# Offline: a local copy of the cluster's logs
sparkplain --app-id application_1700000000000_0042 --from ./logs/j-ABC123 --eventlog ./eventlog

# Check what a run can read, then stop
sparkplain --app-id application_1700000000000_0042 --profile prod --cluster-id j-ABC123 --check

# Print the event a page cites, redacted
sparkplain --app-id application_1700000000000_0042 --eventlog ./eventlog --show events_1:1234`
)

// newRootCmd is the one command sparkplain has. run defines its flags;
// parseArgs runs it.
func newRootCmd(stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     cliUse,
		Long:    cliLong,
		Example: cliExample,
		Args:    cobra.ArbitraryArgs, // run rejects them with a plain message
		// cliUse already shows the flags.
		DisableFlagsInUseLine: true,
	}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	// Parsing stops at the first stray value, as it did with the flag
	// package, so run can name it.
	cmd.Flags().SetInterspersed(false)
	return cmd
}

// parseArgs parses args into cmd's flags through Fang. ran is false when
// the help was asked for and printed; err is a flag mistake, already
// printed. rest is what follows the flags.
func parseArgs(cmd *cobra.Command, args []string) (rest []string, ran bool, err error) {
	cmd.RunE = func(_ *cobra.Command, a []string) error {
		rest, ran = a, true
		return nil
	}
	cmd.SetArgs(longFlags(cmd.Flags(), args))
	err = fang.Execute(context.Background(), cmd,
		// -version prints sparkplain's own version string.
		fang.WithoutVersion(),
		// One command: no hidden man page or completion subcommands.
		fang.WithoutManpage(),
		fang.WithoutCompletions(),
	)
	return rest, ran, err
}

// longFlags lets a long flag take one dash, as the flag package allowed:
// -app-id x and -app-id=x become --app-id x and --app-id=x. pflag would
// read -app-id as the shorthands -a -p -p -… instead. A flag's value is
// never rewritten, and nothing after "--" is.
func longFlags(fs *pflag.FlagSet, args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			return append(out, args[i:]...)
		}
		if len(a) < 3 || a[0] != '-' {
			out = append(out, a)
			continue
		}
		long := a[1] == '-'
		name, _, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
		f := fs.Lookup(name)
		switch {
		case !long && (f != nil || name == "help"):
			out = append(out, "-"+a)
		default:
			out = append(out, a)
		}
		if f != nil && !hasValue && f.NoOptDefVal == "" && i+1 < len(args) {
			i++ // the flag's value, which may start with a dash
			out = append(out, args[i])
		}
	}
	return out
}
