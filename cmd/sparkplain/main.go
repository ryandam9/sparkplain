// Command sparkplain turns one Spark application's logs into a plain-language
// HTML report and a JSON export. See docs/SPEC.md.
package main

import (
	"os"
	"runtime/debug"
)

func main() {
	// A soft memory limit, unless GOMEMLIMIT sets one: the garbage
	// collector then works harder near 1 GiB instead of letting the heap
	// double. On 400,000 tasks with their executors' logs it took peak
	// memory from 2.9 GB to 1.5 GB in the same time (SPEC §4).
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(1 << 30)
	}
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
