package main

import (
	"fmt"
	"io"

	"github.com/ryandam9/sparkplain/internal/sparkplain/yarnlog"
)

// decodeScan prints an HBase scan string decoded, for -decode-scan. It
// reads nothing but the string (or stdin, for "-", which keeps the string
// out of the shell's history) and calls nothing.
func decodeScan(arg string, stdin io.Reader, stdout, stderr io.Writer) int {
	if arg == "-" {
		b, err := io.ReadAll(io.LimitReader(stdin, 16<<20))
		if err != nil {
			fmt.Fprintf(stderr, "sparkplain: reading the scan from stdin: %v\n", err)
			return exitFatal
		}
		arg = string(b)
	}
	sc, err := yarnlog.DecodeScan(arg)
	if err != nil {
		fmt.Fprintf(stderr, "sparkplain: -decode-scan: %v\n", err)
		return exitFatal
	}
	fmt.Fprintln(stdout, "HBase scan")
	for _, f := range sc.Facts() {
		fmt.Fprintf(stdout, "  %-18s %s\n", f[0], f[1])
	}
	if lines := sc.Filter.Lines(); len(lines) > 0 {
		fmt.Fprintln(stdout, "  Filters")
		for _, l := range lines {
			fmt.Fprintln(stdout, "    "+l)
		}
	} else {
		fmt.Fprintf(stdout, "  %-18s %s\n", "Filters", "none")
	}
	return exitOK
}
