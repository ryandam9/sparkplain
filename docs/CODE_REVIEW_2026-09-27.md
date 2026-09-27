
# sparkplain code review — 27 September 2026

**Reviewed branch:** `master`  
**Reviewed commit:** `5e90f37a8a820fcffbb92495237e437e257c8f7c`  
**Review scope:** CLI flow, event-log resolution/parsing, compression handling, EMR/AWS metadata, YARN log collection, redaction, report generation, source embedding, tests, build/release hygiene, performance and operational safety.

## Resolution status

All findings were checked against `master` (`ce546be`) and confirmed before fixing. Each fix is its own commit with the regression tests the review asked for; `make check` passes, and the 1 GB benchmark is unchanged (8.1–8.3 s and about 460 MB peak RSS before and after).

| ID | Status | Commit | Resolution |
|---|---|---|---|
| SP-001 | Fixed | `c250fb1` | `awsmeta.Steps` stays oldest first (the report and charts rely on it) and says so; `narrow` and `stepEventLogDirs` walk it newest first. Tests: 75 chronological steps, per-step `spark.eventLog.dir` order, the ordering contract. |
| SP-002 | Fixed | `3f35506` | New read-only `Store.Head`: `os.Stat` locally; on S3 a listing of at most one key starting with the name, which is the key itself when it exists. Same fields as `List`, no new permission or S3 API method. A trailing `/` skips it. Test: 100,000 unrelated keys, no broad listing. |
| SP-003 | Fixed | `72f6d34` | `source.Bounded` fails with `tooLarge` past a limit instead of stopping quietly. Applied to gzip/bzip2 output and event log parts (`-max-unpacked`, default 50 GiB), plain objects (`-max-size`), zip entries (refused by header over 1 GiB, bounded while reading), with a shared 1 GiB budget for zips held in memory. |
| SP-004 | Fixed | `0acb243` | `redact.Args` hides the argument after a sensitive option given on its own; `redact.Command` splits quote-aware and re-quotes. A fuzz target found a re-quoting bug, fixed and kept as a regression case. |
| SP-005 | Fixed | `b05766c` | Reports 0600, folders sparkplain creates 0700; sharing is a deliberate `chmod`. |
| SP-006 | Fixed | `df4bfd6` | `sc.Err()` checked; an over-long line or read error marks the file cut with a note saying why. |
| SP-007 | Fixed | `3f24e6a` | `source.ContextReader` checks the context before every read, under every fetched object and event log part. Test: a slow store that ignores its context (hangs without the fix). |
| SP-008 | Fixed | `87b983f` | Several logs with no name match is `notFound` (exit 2); a zip's only log is read but a name mismatch must be confirmed by the log's own application ID, else exit 2. Rolling folders are chosen by full path. |
| SP-009 | Fixed | `b1934e7` | Past the entry limit the zip is marked `tooLarge` (partial) with "read the first N of M entries". |
| SP-010 | Fixed | `3f4fe74` | `.github/workflows/ci.yml` runs gofmt, vet, race tests, build and govulncheck on pull requests and master; actions pinned by SHA; govulncheck pinned to v1.8.0 in `make vuln`. Requiring the check before merging is a branch-protection setting for the repository owner. |
| SP-011 | Fixed | `e47cf2c` | README names `<app-id>-report.html` etc. and adds a Supported section. |
| SP-012 | Fixed | `bb8efb3` | Files whose metadata cannot be read stay in the listing, so their read fails visibly; vanished files are still skipped. (Returning a List error would have failed whole sources.) |
| SP-013 | Fixed | `f8ae644` | Unused map removed. |
| SP-014 | Decided | `e47cf2c` | Linux and macOS are the supported platforms; Windows is out of scope (README, SPEC §6). |
| Workers | Fixed | `c132381` | `-workers` must be 1 to 256. |

Not taken up: structured JSON diagnostics, `staticcheck`, a scheduled benchmark job, and the extra fuzz targets beyond redaction. They remain good ideas.

## Executive summary

`sparkplain` has a strong foundation for a young diagnostics tool. The implementation is deliberately streaming in many of the expensive paths, keeps provenance down to file/line, uses ETag preconditions for S3 consistency, has thoughtful caps around explorer/task detail, uses `html/template` plus escaped JSON for report safety, contains substantial fixture-based tests, and has a useful local `make check` gate with race detection and vulnerability scanning.

The most important issues are not general Go style. They are edge cases where the tool can select the wrong EMR step, do much more S3 work than intended, accept uncontrolled decompression expansion, silently truncate ZIP-derived log content, or fail to redact secrets expressed as split command-line arguments. Those are worth fixing before relying on the tool against large production clusters and untrusted/unknown log bundles.

### Finding summary

| ID | Severity | Area | Finding |
|---|---|---|---|
| SP-001 | High | EMR / correctness | Step ordering is oldest-first, while consumers assume newest-first and stop after 50 |
| SP-002 | High | S3 / performance | Event-log lookup lists the entire supplied prefix before targeted application lookups |
| SP-003 | High | Input safety | gzip/bzip2 expansion is unbounded and ZIP entries can be silently truncated |
| SP-004 | High | Security / privacy | Split secret CLI arguments such as `--password secret` are not redacted |
| SP-005 | Medium | Security / privacy | Generated diagnostic reports are explicitly world-readable on Unix |
| SP-006 | Medium | Correctness | Source embedding ignores `bufio.Scanner.Err()` and can silently embed incomplete code |
| SP-007 | Medium | Reliability | Per-object timeout is not a hard deadline for local reads/decompression/callback processing |
| SP-008 | Medium | Correctness | ZIP resolution can fall back to a different application's log |
| SP-009 | Medium | Data completeness | ZIP archives past `MaxEntries` are silently cut without marking the source partial |
| SP-010 | Medium | CI / release | Strong local checks exist but are not enforced by CI; vuln tool version is unpinned |
| SP-011 | Low | Documentation | README output filenames do not match the filenames the CLI actually writes |
| SP-012 | Low | Error handling | LocalStore silently drops files whose metadata cannot be read |
| SP-013 | Low | Maintainability | `run` builds an explicit-flag set that is never used |
| SP-014 | Low | Portability | Atomic output replacement relies on Unix-style `os.Rename` overwrite behaviour |

No issue found in this pass was classified Critical. The High items are all concrete and should be addressed before broad production use.

---

## Detailed findings

### SP-001 — High — EMR step ordering contradicts its consumers

**Evidence**

- `internal/sparkplain/awsmeta/emr.go:160` explicitly sorts `Steps` oldest-first.
- `cmd/sparkplain/logs.go:163-170` says `stepEventLogDirs` consumes steps newest-first but iterates the slice from the front.
- `cmd/sparkplain/logs.go:202-211` says ListSteps is newest-first, iterates from the front, and stops after `maxStepsSearched = 50`.

This creates a functional bug on clusters with many steps. When the event log is unavailable or cannot narrow the candidate window, `narrow` can inspect only the **oldest 50** steps. A recent application can therefore be missed even though its submitting step is present in EMR metadata. The same ordering mismatch makes per-step `spark.eventLog.dir` discovery prefer old step configuration before recent step configuration.

**Suggested fix**

Choose one ordering contract and enforce it everywhere. The simplest option is to make `awsmeta.Steps` return newest-first and update its comment/tests. Alternatively, keep oldest-first as the API contract and iterate backwards in `narrow` and `stepEventLogDirs`.

**Regression tests**

1. Build 75 synthetic steps in chronological order and verify an application submitted by step 74 is included in the 50-step search.
2. Give old and new steps different `spark.eventLog.dir` values and verify the newest applicable location is tried first.
3. Add a test that documents the ordering contract of `awsmeta.Steps`.

---

### SP-002 — High — S3 event-log resolution performs an unnecessary full-prefix listing

**Evidence:** `internal/sparkplain/eventlog/input_store.go:22-55`.

`ResolveStore` starts with:

`st.List(ctx, loc)`

and scans that result only to determine whether `loc` is an exact object. After that, it performs the correctly targeted lookups for:

- `prefix + appID`
- `prefix + "eventlog_v2_" + appID`

When `loc` is a conventional event-log prefix such as `spark-events/`, the first call can enumerate the entire history bucket/prefix before the application-specific queries run. On a busy History Server bucket this can mean thousands or millions of keys, unnecessary pagination, memory use and S3 LIST cost.

This also works against the otherwise good design principle in the project of narrowing log discovery to application-specific prefixes.

**Suggested fix**

Avoid the broad listing.

- If `loc` ends in `/`, treat it as a prefix immediately and perform only application-specific listings.
- For a non-slash location that may be an exact object, add a Store-level exact-object operation (HeadObject/Stat) or a bounded exact-key lookup rather than listing everything below the string.
- Keep the targeted `appID` and `eventlog_v2_appID` searches as the fallback.

**Regression/performance test**

Use a fake store containing a very large unrelated prefix and assert `ResolveStore` never calls `List` with the broad root when the input is clearly a directory/prefix.

---

### SP-003 — High — decompression budgets are incomplete and ZIP truncation can look successful

**Evidence:** `internal/sparkplain/source/fetch.go:93-131`.

`source.Fetch` caps the compressed object, but:

- gzip is handed directly to the callback after `gzip.NewReader` with no decompressed-byte budget;
- bzip2 is likewise unbounded after decompression;
- ZIP entries are wrapped with `io.LimitReader(er, lim.MaxZip)`, but the code does not test whether the entry actually exceeded that limit.

The ZIP behaviour is particularly dangerous for correctness: `io.LimitReader` returns EOF at the limit, so a larger entry can be classified as a successfully read but incomplete log. The caller has no way to distinguish it from a genuinely complete entry of exactly that size.

There is also a concurrency multiplier: `MaxZip` defaults to 256 MiB and the compressed archive is read fully into memory. With the default 16 workers, several large ZIPs can consume multiple GiB before considering decompressed content.

The event-log package does a better job for ZIP entry advertised sizes, but compressed single-file event logs still have no total decompressed-byte limit; only stored size, per-line size and codec-specific block/window bounds exist.

**Suggested fix**

Introduce explicit compressed and decompressed budgets.

- Add `MaxDecompressed` / `MaxEntryBytes` rather than reusing a compressed-size setting ambiguously.
- Wrap decompressed gzip/bzip2 streams in a reader that reads at most `limit+1` and returns `ClassTooLarge` if the extra byte exists.
- For ZIP, reject entries whose `UncompressedSize64` exceeds the entry budget and still retain a runtime `limit+1` guard for dishonest headers.
- Consider a weighted semaphore or aggregate memory budget for ZIP processing rather than a file-count-only semaphore.
- Apply a total decompressed budget to event-log codecs too, while keeping the existing per-line limit.

**Regression tests**

Cover a tiny gzip/bzip2 payload that expands past the limit, a ZIP entry whose uncompressed size exceeds the limit, a forged/misreported archive size, and multiple concurrent large ZIP inputs.

---

### SP-004 — High — split secret command-line values can reach reports unredacted

**Evidence:** `internal/sparkplain/redact/redact.go:145-156` and `internal/sparkplain/awsmeta/emr.go:141-146`.

`redact.Args` understands a self-contained `key=value` argument. It processes every other argument independently with `Text`. Therefore an argument vector like:

`["--db-password", "super-secret-value"]`

keeps the option name but also keeps the following value because the second string has no secret-looking key in it. Similar forms include `--token value`, `--secret value` and application-specific credential flags.

EMR step arguments are copied into the model through `redact.Args`, and controller log commands use `redact.Command`, which simply splits on spaces and inherits the same contextual limitation.

**Suggested fix**

Make argument redaction stateful.

- Recognise `--name=value` and `name=value`.
- When an option name is sensitive, redact the next token when that option takes a separate value.
- Preserve known non-secret identifiers where appropriate; the current redactor intentionally errs on the side of hiding, so that policy can remain conservative.
- For `Command`, avoid pretending that `strings.Split(cmd, " ")` is shell parsing. Either tokenize conservatively with a small quote-aware parser or redact sensitive option/value patterns directly in the original text.

**Regression tests**

Add cases for:

- `--password secret`
- `--token secret`
- `--secret-key secret`
- quoted command strings
- ordinary non-sensitive `--name value` pairs that must remain visible.

---

### SP-005 — Medium — report files are explicitly world-readable

**Evidence:** `cmd/sparkplain/run.go:356` and `cmd/sparkplain/run.go:466`.

The output directory is created with `0755` and every generated report is changed to `0644` before rename. These reports are intentionally redacted, but can still contain operationally sensitive data: usernames, application names, source paths/code, hostnames, cluster IDs, AWS resource names, failure details and selected log lines.

On a shared Unix host, `0644` makes that data readable by other local users. The explicit `Chmod(0644)` also overrides the safer permissions initially produced by `os.CreateTemp`.

**Suggested fix**

Default to:

- output directories: `0700`
- report files: `0600`

If broad sharing is desired, expose an explicit option or document a deliberate `chmod` after generation. Secure-by-default is preferable for a diagnostic tool.

**Regression test**

Create output in a temporary directory and assert mode bits on Unix.

---

### SP-006 — Medium — source embedding ignores scanner failures

**Evidence:** `internal/sparkplain/report/source.go:174-191`.

`LoadSourcesFrom` configures `bufio.Scanner` with a 1 MiB maximum token, then loops over `sc.Scan()` but never inspects `sc.Err()`. A source file containing a line over 1 MiB, or a read error mid-stream, simply stops. The partially embedded file is then returned without a failure note and without necessarily setting `Cut`.

That undermines the explorer's provenance promise because a source file can look complete when it was not.

**Suggested fix**

Check `sc.Err()` after the loop and either:

- return the error for explicitly supplied `-source` files; or
- mark the source partial/cut and append a clear note.

A custom bounded line reader would make the distinction between "line too large" and an I/O failure easier to report.

**Regression test**

Use a file with a >1 MiB line and assert that the result is explicitly marked incomplete.

---

### SP-007 — Medium — `PerObject` timeout is not a hard processing deadline

**Evidence**

- `internal/sparkplain/source/fetch.go:77-79` creates a per-object context.
- `internal/sparkplain/source/local.go:63-76` accepts the context in `Open` but does not consult it.
- `internal/sparkplain/eventlog/lines.go:127` checks cancellation only every 4096 completed lines.

The context bounds AWS I/O reasonably well, but does not guarantee a wall-clock deadline for local-file reads, CPU-heavy decompression, or callback processing. A local large/compressed file can therefore continue past `PerObject` while `Fetch` waits on the goroutine.

**Suggested fix**

Use a context-aware reader wrapper that checks `ctx.Err()` while reading, including local stores and decompression output. Long-line loops should also check cancellation periodically while consuming buffer fragments rather than only after completed lines.

**Regression test**

Use a deliberately slow reader/local store and assert `Fetch` returns close to the configured deadline.

---

### SP-008 — Medium — History Server ZIP fallback can select another application's log

**Evidence:** `internal/sparkplain/eventlog/input.go:253-288`.

For rolling directories, if no directory matches `appID`, the code falls back to every rolling directory. For single logs, if no candidate matches `appID`, it falls back to every non-status entry. It then picks one.

`run.go` catches a mismatch when the parsed log contains an Application Start event with an ID. But if that event is absent/corrupt while other events are readable, the parsed application ID can be empty; later the requested ID is filled into the report. That leaves a path where a report can be produced from the wrong log.

There is also a path-construction fragility when multiple rolling directories live under different parents: the code picks a basename and joins it to the parent of the lexicographically last key, which need not be the selected directory's parent.

**Suggested fix**

When an application ID is supplied:

- prefer exact application-named entries;
- if there is no match and the archive contains multiple plausible logs, return `notFound` rather than guessing;
- only permit a one-entry fallback if needed for compatibility, and validate the parsed application's identity before accepting it;
- select rolling-directory candidates using the full path, not by recombining a basename with another candidate's parent.

**Regression tests**

Create archives with two applications, no matching application, missing Application Start, and rolling logs under different parent directories.

---

### SP-009 — Medium — ZIP `MaxEntries` truncation is silent

**Evidence:** `internal/sparkplain/source/fetch.go:116-119`.

When the archive reaches `MaxEntries`, the loop simply `break`s. The resulting source can still be reported as successfully read. For log bundles, this can hide relevant entries beyond the cap without telling the user that coverage is incomplete.

**Suggested fix**

When the cap is reached and additional entries exist, return/record a partial result with an explicit "entry limit reached" reason. If partial reading is intentional, expose the number skipped.

**Regression test**

Build a ZIP with `MaxEntries+1` files and assert the source is marked partial rather than read/complete.

---

### SP-010 — Medium — quality gates are local-only and not reproducible enough

The repository has a solid local `make check` target:

- gofmt check
- `go vet`
- `go test -race`
- build
- `govulncheck`

However, the reviewed tree has no GitHub Actions workflow and the reviewed master commit has no associated Actions run. This means a pull request can merge without those checks being enforced.

In addition, `Makefile:71` runs:

`go run golang.org/x/vuln/cmd/govulncheck@latest ...`

when the binary is absent. `@latest` makes the same commit capable of producing different build/check results over time.

**Suggested fix**

Add CI for Linux at minimum:

1. set up the Go version from `go.mod`;
2. `gofmt -l` failure check;
3. `go vet ./...`;
4. `go test -race -count=1 ./...`;
5. `go build ./cmd/sparkplain`;
6. pinned `govulncheck`;
7. optionally `staticcheck`.

Then require that workflow before merging to `master`. Pin tool versions used by CI.

The existing 1 GB synthetic event-log generator is also a good basis for a scheduled/manual performance regression job, rather than running that heavy test on every PR.

---

### SP-011 — Low — README documents the wrong output filenames

**Evidence:** `README.md:37-43` versus `cmd/sparkplain/run.go:417-423`.

README says the output directory contains:

- `report.html`
- `report.json`
- `explorer.html`

The CLI actually uses `outputName` and writes:

- `<app-id>-report.html`
- `<app-id>-report.json`
- `<app-id>-explorer.html`

This is likely to confuse scripts and users looking for the documented paths.

**Suggested fix**

Update README examples to use the application-prefixed names and mention why the prefix prevents collisions.

---

### SP-012 — Low — LocalStore hides metadata errors

**Evidence:** `internal/sparkplain/source/local.go` inside `List`.

When `d.Info()` fails, the walker returns `nil` and silently omits that file. Permission/race/filesystem errors on individual files therefore do not appear in the Sources panel even though the package otherwise makes a strong effort to classify missing access.

**Suggested fix**

Return the metadata error, or accumulate per-file listing errors so the source is reported partial rather than silently incomplete.

---

### SP-013 — Low — explicit flag tracking is dead code

**Evidence:** `cmd/sparkplain/run.go:100-101`.

`run` builds a map of flags explicitly visited:

`set := map[string]bool{}`

but never uses it. This looks like a remnant of an earlier config-precedence implementation and makes future readers wonder whether explicit-vs-config precedence is incomplete.

**Suggested fix**

Delete it, or use it deliberately if config values are meant to be ignored whenever a flag was explicitly supplied even when its value is the zero value.

---

### SP-014 — Low — output replacement has a Windows portability edge

**Evidence:** `cmd/sparkplain/run.go:457-473`.

The temp-file + rename pattern is good for avoiding partially written reports. On Unix, `os.Rename` replaces an existing destination atomically. On Windows, replacing an existing destination can fail. Re-running sparkplain into the same output folder can therefore behave differently across platforms.

**Suggested fix**

If Windows is a supported client platform, use a small replace helper with platform-appropriate semantics and tests. If Linux/macOS are the intentional supported platforms, state that in README and avoid carrying accidental portability expectations.

---

## Additional improvements

These are lower-risk design improvements rather than confirmed bugs.

### Validate resource-control flags

`-workers` accepts any integer. Put a sensible upper bound on it (for example 1–256, configurable if needed). A concurrency count should be a safety limit, not another route to exhausting file descriptors/memory.

Similarly, distinguish limits in user-facing names:

- compressed object size;
- decompressed object/entry size;
- maximum event line;
- ZIP compressed-buffer size;
- archive entry count.

That makes failures easier to understand than overloading "max size."

### Make partial-data semantics impossible to miss

One of sparkplain's best design choices is its Sources panel. Extend that principle to every cap:

- entry caps;
- source-code line/byte caps;
- event-log sampling caps;
- decompressed-size limits;
- CloudTrail event caps.

Whenever a cap changes the data set, ensure the source or section reports `partial` rather than looking complete.

### Add fuzz/property tests around input boundaries

The existing redaction fuzz test is a good start. High-value additional fuzz targets are:

- event-log codec sniffing and codec readers;
- rolling filename/attempt selection;
- ZIP/path resolution;
- `ParseSize`;
- source-tail matching;
- shell/argument redaction;
- JSON event-name/key extraction.

Useful invariants include "never panic", "never allocate based only on untrusted declared length above the configured cap", and "redaction is idempotent."

### Add golden CLI tests for failure/partial semantics

The project already tests many online/offline scenarios. Add a small matrix that locks down:

| Scenario | Expected exit | Expected source state |
|---|---:|---|
| requested app not in multi-log ZIP | 2 or 3 by policy | notFound/error |
| one compressed log exceeds decompressed budget | 3 | tooLarge/partial |
| one YARN log ZIP exceeds entry cap | 3 | partial |
| source file scanner fails | 3 or explorer note | partial |
| recent submitting step is beyond first 50 chronological steps | 0/3 based on other sources | correct step selected |

### Consider structured diagnostic logging

The CLI's human-readable stderr is useful. A `-log-format=json` or `-diagnostics-json` option could make automation easier, especially because the report itself already has structured source/error classes.

### Document support guarantees

README is clear about the first Spark/EMR target, but a small compatibility section would help:

- tested OSes for the CLI;
- Go toolchain required to build;
- Spark versions/EMR releases covered by fixtures;
- supported event-log codecs/layouts;
- AWS permissions by optional feature;
- behaviour for running/in-progress applications.

---

## What is already strong

The review found several design choices worth preserving:

1. **Streaming parser architecture.** Event logs are processed line-by-line rather than loaded wholesale.
2. **Provenance.** Findings and values retain file/line source information, which is excellent for a diagnostic tool.
3. **Bounded detail structures.** Running tasks, plan text, samples, stage×executor cells and other high-cardinality structures have explicit caps.
4. **S3 consistency protection.** Listed ETags are sent with `If-Match` for normal Store reads, detecting objects that changed during analysis.
5. **Failure degradation.** AWS permission failures generally degrade into Sources-panel coverage instead of aborting the whole report.
6. **Redaction-first model.** Common sensitive settings, AWS access-key IDs, URL passwords and unsafe Unicode/control characters are removed before presentation.
7. **HTML safety.** The main report uses `html/template`; explorer data is JSON-marshalled with HTML escaping before embedding; chart/SVG text paths reviewed here escape log-controlled labels.
8. **Fixture depth.** The repo includes plain, rolling, in-progress and multiple compressed event-log fixtures plus EMR/YARN fixtures.
9. **Race testing.** `go test -race` in the normal local quality gate is the right default for the concurrent fetch paths.
10. **Atomic-write intent.** Writing to a temp file before rename avoids exposing half-rendered reports on supported platforms.

---

## Suggested implementation order

### First pass — correctness and data safety

1. **SP-001** step ordering
2. **SP-004** split-argument redaction
3. **SP-003** decompression/ZIP limits
4. **SP-008/SP-009** ZIP selection and partial semantics

These affect whether the report is about the right application and whether sensitive/incomplete data can be presented incorrectly.

### Second pass — scale and operational hardening

1. **SP-002** targeted S3 event-log discovery
2. **SP-005** secure file permissions
3. **SP-006/SP-007** source-read errors and true timeouts
4. worker validation / aggregate memory control

### Third pass — engineering hygiene

1. **SP-010** CI and pinned tooling
2. **SP-011** README filename correction
3. **SP-012/SP-013/SP-014** smaller cleanup/portability items
4. fuzz and performance regression automation

---

## Validation plan after fixes

Before considering the findings closed:

`make check`

Then add targeted tests for SP-001 through SP-009 and run the synthetic large-log benchmark. For performance-sensitive changes, capture at least:

- wall time;
- peak RSS;
- S3 List/Get call counts in fake-store tests;
- number of goroutines/workers;
- generated report size.

A particularly useful acceptance test is a fake S3 prefix containing a very large number of unrelated applications. Resolving one app should perform only app-specific listings and should have memory/time approximately independent of the number of unrelated keys.

---

## Review limitations

This review is a source-level review of the referenced `master` commit through the connected GitHub repository interface. I inspected the implementation and existing tests but did **not** execute the repository's test suite or benchmark in a checked-out runtime during this review. The repository currently has no GitHub Actions run attached to the reviewed commit, so there is no remote CI result to use as an execution signal.

That limitation is why the findings above distinguish concrete code-path bugs from suggested hardening, and why the validation plan calls out the exact regression tests that should accompany fixes.
