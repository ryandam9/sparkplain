// Command benchcheck runs a command, typically sparkplain on a log from
// scripts/benchlog, and fails when it takes longer or uses more memory than
// the SPEC §8 budget allows for that log: 60 s and 1 GB of RAM per GB of
// event log, scaled to the log's size. CI runs it (make bench) so a change
// that makes parsing much slower or stops it streaming turns the build red.
//
//	go run ./scripts/benchcheck -log out/bench/app -- ./sparkplain -app-id app -eventlog out/bench/app -out out/bench/report
//
// Peak memory is the child's maximum resident set size, from the operating
// system (Linux and macOS).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"
)

func main() {
	logPath := flag.String("log", "", "the event log the command reads, to scale the budget")
	secPerGB := flag.Float64("seconds-per-gb", 60, "time budget per GB of log (SPEC §8: 60 s)")
	mbPerGB := flag.Float64("rss-mb-per-gb", 1024, "peak memory budget per GB of log (SPEC §8: under 1 GB)")
	flag.Parse()
	if *logPath == "" || flag.NArg() == 0 {
		log.Fatal("usage: benchcheck -log <event log> -- <command> [args]")
	}
	st, err := os.Stat(*logPath)
	if err != nil {
		log.Fatal(err)
	}
	gb := float64(st.Size()) / (1 << 30)
	maxTime := time.Duration(*secPerGB * gb * float64(time.Second))
	maxRSS := *mbPerGB * gb

	cmd := exec.Command(flag.Arg(0), flag.Args()[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	start := time.Now()
	runErr := cmd.Run()
	took := time.Since(start)
	rss := peakMB(cmd.ProcessState)
	fmt.Printf("benchcheck: %.2f GB log in %s (budget %s), peak memory %.0f MB (budget %.0f MB)\n",
		gb, took.Round(10*time.Millisecond), maxTime.Round(10*time.Millisecond), rss, maxRSS)
	// sparkplain exits 3 when some source is missing, which a synthetic log
	// with no cluster behind it always is; anything else is a failure.
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
			log.Fatalf("command failed: %v", runErr)
		}
	}
	bad := false
	if took > maxTime {
		fmt.Println("benchcheck: over the time budget")
		bad = true
	}
	if rss > maxRSS {
		fmt.Println("benchcheck: over the memory budget")
		bad = true
	}
	if bad {
		os.Exit(1)
	}
}

// peakMB is the process's maximum resident set size in MiB. Linux reports
// ru_maxrss in KiB, macOS in bytes.
func peakMB(ps *os.ProcessState) float64 {
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil {
		log.Fatal("no resource usage for the command on this system")
	}
	if runtime.GOOS == "darwin" {
		return float64(ru.Maxrss) / (1 << 20)
	}
	return float64(ru.Maxrss) / 1024
}
