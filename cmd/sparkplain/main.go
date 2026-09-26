// Command sparkplain turns one Spark application's logs into a plain-language
// HTML report and a JSON export. See docs/SPEC.md.
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
