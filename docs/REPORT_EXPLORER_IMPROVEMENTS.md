# Report and Explorer improvement proposal

**Date:** 27 September 2026  
**Scope:** `report.html` and `explorer.html`  
**Base branch:** `master`

## Goal

The current reporting layer already contains a strong set of Spark diagnostics: application summary, findings, executor/job timelines, node CPU, YARN/container views, stage duration/data/spill charts, executor time breakdown, heap views, task histograms/scatter plots, SQL timelines, configuration, logs and source-code evidence.

The next step should not be "add as many charts as possible." The goal should be to make the UI answer diagnostic questions faster:

1. **Where is the bottleneck?**
2. **Why is it slow?**
3. **Is the problem data skew, resource pressure, scheduling, I/O, GC, or executor/node behaviour?**
4. **Which stage/job/query actually controls wall-clock runtime?**
5. **What should the user inspect next?**

This proposal separates the responsibilities of the two generated pages:

- **Report:** concise, diagnostic, opinionated, limited to the most useful visuals.
- **Explorer:** interactive evidence and deep investigation.

---

# 1. Product direction

## Report philosophy

The report should answer:

- What happened?
- What are the important problems?
- What constrained the run?
- Which jobs/stages/executors deserve investigation?
- What evidence supports the conclusion?

The report should avoid becoming a large Spark History Server clone. Keep the number of major visualisations intentionally limited.

Recommended target: **6–8 primary charts/diagrams** plus compact tables.

## Explorer philosophy

The Explorer should answer:

- Show me the evidence.
- Let me correlate metrics.
- Let me move from application → query → job → stage → task/executor.
- Let me inspect the same data using different dimensions.
- Let me compare healthy and unhealthy parts of the run.

The Explorer can support many more visualisations because drill-down and filtering are its purpose.

---

# 2. Recommended priority

| Priority | Feature | Surface | Main question answered |
|---|---|---|---|
| P0 | Stage Health Map | Report + Explorer | Which stages deserve attention? |
| P0 | Stage × Executor Heatmap | Explorer | Is one executor/node behaving differently? |
| P0 | Task Time Breakdown | Report + Explorer | Why is a stage slow? |
| P1 | Unified Run Timeline | Explorer | What happened at the same time? |
| P1 | Critical Path Graph | Report + Explorer | What actually determined runtime? |
| P1 | Task Distribution Comparison | Explorer | Which stages have skew/stragglers? |
| P1 | Correlation Scatter Controls | Explorer | Is runtime explained by data volume / shuffle / waits? |
| P2 | Memory Composition View | Report + Explorer | Where did memory go? |
| P2 | SQL → Job → Stage Execution Graph | Explorer | How does SQL map to Spark execution? |
| P2 | Executor Churn View | Explorer | Is dynamic allocation or executor loss expensive? |
| P2 | Resource Utilisation Scorecard | Report | Was the cluster efficiently used? |
| P3 | Replace Google Charts | Explorer | Make the Explorer fully offline |

---

# 3. Stage Health Map

## Why

The current report can show longest stages, stage data, spill and task details, but the user still needs to mentally combine several views.

A Stage Health Map would provide one application-wide diagnostic view.

## Proposed encoding

Each bubble is a stage.

- **X axis:** stage duration
- **Y axis:** total data moved
  - input
  - shuffle read
  - shuffle write
  - output
- **Bubble size:** task count
- **Bubble emphasis:** failure, spill or severe skew
- **Tooltip:** stage ID/name, duration, task count, input, shuffle, spill, p50/p95/max task duration
- **Click:** open the stage in Explorer

Optional modes:

- Duration vs data moved
- Duration vs spill
- Duration vs task count
- Duration vs CPU share

## Diagnostic interpretation

Examples:

- **Long duration + little data:** investigate scheduler delay, GC, Python/UDF work, network waits or under-parallelism.
- **Long duration + large data:** likely genuinely expensive work; compare throughput.
- **Large bubble + short duration:** healthy parallelism.
- **Small bubble + long duration:** possible insufficient partitions or serial work.
- **High spill:** memory/partition sizing pressure.

## Data already available

Most required values already exist in stage totals/distributions.

This should therefore be primarily a rendering/UI change rather than a new collection feature.

---

# 4. Stage × Executor Heatmap

## Why

This is the most important Explorer addition.

The current model already tracks stage × executor cells. A heatmap would expose:

- one executor consistently slower than peers;
- a problematic EC2 node;
- uneven task placement;
- localized GC pressure;
- localized shuffle/network pressure;
- executors carrying disproportionate input;
- failed-task concentration.

## Layout

Rows:

- executors

Columns:

- stages

Cell value selectable from:

- task runtime
- CPU time/share
- GC time/share
- input bytes
- shuffle read
- shuffle write
- disk spill
- peak heap
- failed tasks
- task count

## Interaction

Controls:

`Metric: [Runtime ▼]`

`Normalize: [Absolute | Per task | % of stage]`

Click a cell:

`Application > Stage 42 > Executor 17`

The details pane should show:

- tasks
- runtime
- CPU
- GC
- input
- shuffle
- spill
- peak heap
- failures
- node/instance
- source evidence

## Scaling

Large applications need limits.

Recommended defaults:

- top 30 stages by runtime or critical-path relevance;
- top 50 executors by task runtime;
- searchable/filterable;
- "show all" only when under a safe cell count.

The model already has a stage × executor cell cap, so the UI should clearly display when heatmap coverage is partial.

---

# 5. Task Time Breakdown

## Why

The existing stage charts identify slow stages. This chart explains **why** they are slow.

## Proposed chart

100% stacked horizontal bar by stage.

Components:

- CPU / compute
- garbage collection
- shuffle fetch wait
- scheduler delay
- task deserialization
- result serialization
- getting result
- remainder / other

Example:

```
Stage 42  ██████████████████▒▒▒▒▓▓░░
          CPU 62% | Fetch 14% | Scheduler 10% | GC 8% | Other 6%
```

## Two modes

### Percentage mode

Best for comparing stage behaviour.

### Absolute mode

Best for understanding where application time is spent.

## Interpretation

- high CPU → compute-bound;
- high GC → memory pressure / object churn;
- high fetch wait → shuffle/network pressure;
- high scheduler delay → insufficient slots, skew or scheduling overhead;
- high deserialize/serialization → very small tasks / serialization overhead;
- large "other" → inspect Python, external I/O or unsupported timing components.

## Report placement

Show the top 8–12 stages by task runtime.

Explorer can expose all stages.

---

# 6. Task Distribution Comparison

## Why

The current per-stage task-duration histogram is useful after the user already knows which stage to inspect.

The missing view is a cross-stage comparison of skew.

## Proposed visual

Percentile/box style rows for the most expensive stages.

Display:

- min
- p25
- p50
- p75
- p95
- max

Example:

```
Stage 31  ├──[ p25 | p50 | p75 ]────● p95────● max
Stage 44  ├─[p25|p50|p75]──●
Stage 57  ├──[p25|p50|p75]────────────────────●
```

## Useful derived indicators

Add compact labels:

- `max / p50`
- `p95 / p50`
- tasks
- speculative attempts
- skew warning

## Why this is better than another histogram

It allows the user to compare many stages at once and immediately identify long-tail behaviour.

---

# 7. Improve the current task scatter plot

The existing "task start vs task duration" chart is excellent and should be kept.

Make it reusable with axis selectors.

## Suggested Y-axis options

- duration
- input bytes
- shuffle read
- records read
- spill
- GC time
- scheduler delay
- fetch wait
- result size

## Suggested X-axis options

- task start time
- input bytes
- records read
- shuffle read
- partition index

## High-value combinations

### Input bytes vs duration

Answers whether slow tasks are explained by data skew.

### Records vs duration

Useful when record sizes vary little.

### Shuffle read vs fetch wait

Highlights network/shuffle bottlenecks.

### GC vs peak execution memory

Highlights memory pressure.

### Start time vs duration

Keep as the default because it shows waves/stragglers clearly.

## Sample awareness

The chart must continue to state clearly whether it is:

- every task;
- a sample;
- sample + slowest tasks.

Do not visually imply full coverage when sampling is active.

---

# 8. Unified Run Timeline

## Why

Today, jobs, executors, running-task slots, SQL queries and CloudWatch metrics are viewed separately.

The user often needs to answer:

> What else was happening when the problem occurred?

## Proposed layout

All tracks share a common time axis.

Tracks:

1. SQL queries
2. jobs
3. stages
4. executors alive
5. tasks running
6. available task slots
7. node/cluster CPU
8. GC pressure
9. container pending/allocated
10. executor loss/exclusion markers
11. failures/retries
12. optional I/O/shuffle activity

Example concept:

```
12:00       12:05       12:10       12:15
|-----------|-----------|-----------|

Query 14    █████████████████
Job 7          ███████████
Stage 31       █████
Stage 32             ███████

Executors  ─ 12 ─── 20 ───── 18 ─────
Tasks      ▂▄██████████▆▃___██████
CPU        ▁▃▇██████████▅▂__▅█████
GC         ▁▁▂▃▂████▅▂▁_____▂▂▁
Pending    __________▃███▅▂________

                       ▲
                  executor lost
```

## Interaction

A vertical hover cursor should display all values at the same timestamp.

Zooming the time axis should zoom every track together.

Selecting a job/stage/query should highlight related tracks.

This has more diagnostic value than adding several independent time-series charts.

---

# 9. Critical Path Graph

## Why

"Longest stage" is not always the stage that determines wall-clock time. Parallel branches can be expensive without delaying completion.

The model already exposes a critical path.

## Report view

A compact DAG:

- stages as nodes;
- dependencies as edges;
- critical-path nodes/edges emphasized;
- node label: stage ID + duration;
- hover: tasks, data, spill, skew;
- click: Explorer.

Below the graph:

- application duration;
- critical path duration;
- top critical-path stages;
- non-critical expensive stages.

## Explorer view

Support:

- full job DAG;
- highlight critical path;
- filter to one job/query;
- overlay stage state/failure;
- show data/shuffle volumes on edges where meaningful.

## Important terminology

Explain clearly that critical path means:

> the dependency chain that constrained completion

rather than simply "the stages with the largest durations."

---

# 10. SQL → Job → Stage execution graph

## Why

Spark SQL users often start from a query, while Spark's execution evidence is distributed across SQL plans, jobs and stages.

Explorer should make that relationship explicit.

## Suggested hierarchy

```
SQL Query 18
   |
   +-- Job 21
   |     +-- Stage 44
   |     +-- Stage 45
   |
   +-- Job 22
         +-- Stage 46
         +-- Stage 47
```

Selecting a stage should highlight the related SQL plan nodes when the mapping is available.

Selecting a SQL plan node should show:

- related metrics;
- jobs;
- stages;
- code location;
- data source;
- adaptive metrics.

This would make the Explorer substantially more useful than the normal "separate SQL tab + jobs tab" pattern.

---

# 11. Memory Composition View

## Why

Spark memory is difficult to understand from numbers alone.

The existing heap and YARN charts should remain, but add an explicit hierarchy.

## Executor/container view

```
Executor container — 8 GiB

| Java heap 6 GiB | Overhead 1 GiB | PySpark 768 MiB | Other |
                 ^
                 peak observed heap
```

Show:

- configured executor memory;
- overhead;
- PySpark memory;
- off-heap;
- observed heap;
- observed RSS where available.

## Node view

```
EC2 RAM
| OS/daemons | YARN available ------------------------------------ |

YARN
| Executor 1 | Executor 2 | Executor 3 | unusable gap | free |
```

This should visually distinguish:

1. EC2 memory;
2. YARN memory;
3. container allocation;
4. executor heap;
5. measured peak usage.

That hierarchy is much easier to understand visually than as separate values.

---

# 12. Executor Churn View

## Why

Dynamic allocation can be healthy, but repeated executor startup/removal can also add latency.

## Proposed chart

Timeline of executor lifetimes plus a compact rate/summary:

- executors added;
- executors removed;
- killed/lost;
- idle removals;
- startup duration;
- peak executor count;
- requested vs registered delay when available.

Optional derived values:

- median executor lifetime;
- median startup time;
- executors alive < 2 minutes;
- churn count per 10 minutes.

## Useful correlation

Overlay or align with:

- pending containers;
- task slots;
- job/stage boundaries.

This makes it obvious whether scaling lag delayed work.

---

# 13. Resource Utilisation Scorecard

## Why

The report needs a compact answer to:

> Did the application use what it was given?

## Suggested card

```
CPU utilisation         ███████░░░ 72%
Task-slot utilisation   ██████░░░░ 61%
Peak heap               █████████░ 89%
YARN memory occupancy   ████████░░ 83%
Shuffle spill           18.2 GiB
Executor losses         2
```

Avoid reducing everything to one synthetic "health score." Individual metrics are more transparent and actionable.

Useful explanatory text should accompany each metric.

---

# 14. Report visualisation set

Recommended **Report** visuals:

1. Run anatomy
2. Stage Health Map
3. Critical Path
4. Task Time Breakdown
5. Executor timeline
6. Cluster container pressure
7. Node/resource utilisation
8. Memory composition

Everything else should generally remain a table/fact or move to Explorer.

This prevents the report from becoming visually noisy.

---

# 15. Explorer navigation

Recommended primary navigation:

```
Overview
Timeline
Jobs
Stages
SQL
Executors
Nodes
Storage & I/O
Configuration
Logs
Code
Sources
```

## Overview

The Explorer overview should be a diagnostic dashboard, not another report copy.

Suggested layout:

```
+-----------------------------------------------------------+
| APPLICATION                                               |
| 22m 07s | 20 executors | 18.4 TB read | 3 warnings       |
+-----------------------------+-----------------------------+
| Stage Health Map            | Resource utilisation        |
|                             | CPU     74%                 |
|                             | Memory  89%                 |
|                             | Slots   63%                 |
+-----------------------------+-----------------------------+
| Unified run timeline                                      |
+-----------------------------------------------------------+
| Critical path                                             |
| Stage 7 -> Stage 12 -> Stage 18             18m 42s       |
+-----------------------------------------------------------+
```

---

# 16. Context-aware drill-down

Explorer should carry context between pages.

Examples:

```
Application > Job 7 > Stage 42
```

and:

```
Application > Stage 42 > Executor 17
```

A stage detail header should show:

- duration
- tasks
- input
- shuffle
- spill
- CPU share
- GC share
- skew
- executors
- status

Then tabs/sections:

```
Overview | Tasks | Executors | Data | Memory | SQL | Evidence
```

Clicking a chart element should navigate to the corresponding context rather than merely updating a tooltip.

---

# 17. Linked highlighting

Cross-highlighting would materially improve Explorer usability.

Examples:

- click Stage 42 → highlight its job and SQL query;
- click Executor 17 → highlight its host/node;
- click a failed task → highlight its executor and stage;
- click a SQL query → highlight all associated jobs/stages;
- click a timeline interval → filter tables to that period.

This is a stronger improvement than adding independent charts that do not communicate with each other.

---

# 18. Filters

Useful global filters:

- time range;
- job;
- stage;
- SQL query;
- executor;
- host/node;
- status;
- failed only;
- critical-path only.

Useful stage filters:

- slowest;
- skewed;
- spilled;
- failed/retried;
- high GC;
- high scheduler delay;
- high shuffle.

---

# 19. Chart guidance

One of the strongest existing UX features is the "How to read it" text beneath charts.

Keep this pattern.

Every new visual should state:

1. **What it shows**
2. **How to read it**
3. **Whether data is sampled**
4. **Whether data is incomplete**
5. **Where the data came from**

Avoid unexplained generic chart titles such as "Stage metrics."

Prefer titles that communicate the diagnostic question:

- "Which stages moved the most data?"
- "Where executor time went"
- "Which stages had long-tail tasks?"
- "Did executors use the heap they were given?"
- "Which executor behaved differently?"

---

# 20. Colour semantics

Keep semantic colours consistent across all pages.

Suggested semantic categories:

- normal/succeeded
- warning
- failure/critical
- neutral/unknown
- selected/highlighted

Do not assign a new arbitrary colour to every metric.

For multi-series charts, use a stable metric palette so that, for example:

- input;
- shuffle read;
- shuffle write;
- spill;
- CPU;
- GC

retain the same visual identity across charts.

Also ensure all distinctions remain understandable without colour alone.

---

# 21. Avoid too many pie/donut charts

Do not add pie charts for:

- executor status;
- stage status;
- time breakdown;
- memory usage;
- task locality.

Bars and stacked bars make comparisons much easier.

A donut could be acceptable for one very small part-to-whole summary, but it should not become a default chart type.

---

# 22. Full offline Explorer

## Current state

The report is self-contained.

The Explorer bundles D3 locally for anatomy but loads Google Charts dynamically from:

`https://www.gstatic.com/charts/loader.js`

This means many Explorer charts require internet access.

## Recommendation

Make `explorer.html` fully self-contained.

Since D3 is already bundled, migrate Google Charts visualisations gradually to local SVG/D3 renderers.

Benefits:

- works on restricted corporate machines;
- works on jump boxes/bastion hosts;
- works in isolated AWS environments;
- no third-party runtime dependency;
- deterministic rendering;
- easier version pinning;
- better control over linked interactions and shared timelines.

## Suggested migration order

1. horizontal stacked bars
2. line/area charts
3. scatter plots
4. histograms
5. timelines
6. unified timeline

Do not rewrite everything in one PR. Replace one chart family at a time.

---

# 23. Performance constraints

Explorer can become very large for real workloads.

All new views should respect explicit budgets.

Recommended principles:

- aggregate before rendering;
- virtualize long tables;
- cap default heatmap cells;
- sample task scatter data;
- keep slowest/outlier tasks separately;
- lazy-render charts when their tab/section becomes visible;
- reuse the existing limits model;
- make every cap visible to the user.

Do not silently omit data.

---

# 24. Accessibility

Every chart should have:

- meaningful title;
- text description;
- keyboard-focusable interactive elements where practical;
- tabular fallback or equivalent textual summary;
- labels not dependent on colour alone.

The report already contains good explanatory prose; preserve that standard as the chart set grows.

---

# 25. Suggested implementation phases

## Phase 1 — highest diagnostic value

### 1. Stage Health Map

Low model impact, high report/explorer value.

### 2. Stage × Executor Heatmap

Uses data already collected in explorer stage cells.

### 3. Task Time Breakdown

Uses task totals already available.

### 4. Task percentile comparison

Uses existing stage distributions.

These four features can be implemented without redesigning the entire Explorer.

## Phase 2 — investigation workflow

### 5. Unified Run Timeline

Requires coordinated rendering and shared time scales.

### 6. Critical Path Graph

Expose existing critical-path data visually.

### 7. Linked highlighting and breadcrumb drill-down

Connect charts, tables and detail pages.

## Phase 3 — deeper execution mapping

### 8. SQL → Job → Stage graph

### 9. Memory composition

### 10. Executor churn analysis

## Phase 4 — rendering architecture

### 11. Replace Google Charts

Move to local D3/SVG chart components incrementally.

### 12. Make Explorer fully offline

Remove the final external chart-loader dependency.

---

# 26. Suggested component architecture

Avoid implementing each new chart as a standalone block of one-off rendering code.

Create reusable chart primitives:

- time scale / synchronized cursor
- horizontal bar
- stacked horizontal bar
- percentile range
- scatter plot
- heatmap
- timeline lane
- tooltip
- legend
- chart frame
- empty/partial state
- drill-down link

This would reduce duplication and make the Google Charts migration easier.

---

# 27. Data model additions

Many proposed charts can use existing data.

Potential additions that may be worthwhile:

## Stage summary

Add or expose derived values:

- total data moved;
- CPU share;
- GC share;
- scheduler delay share;
- fetch-wait share;
- p95/p50 ratio;
- max/p50 ratio;
- critical-path flag;
- critical-path contribution.

## Executor summary

Derived values:

- lifetime;
- startup latency;
- task runtime;
- CPU share;
- GC share;
- stage count;
- node identity;
- short-lived flag.

Prefer deriving these during analysis/serialization instead of duplicating complex calculations in JavaScript.

---

# 28. Testing recommendations

## Unit tests

Test derived metrics used by the new charts.

Examples:

- stage data moved;
- stage time component shares;
- skew ratios;
- critical-path membership;
- executor lifetime/churn.

## Golden/snapshot tests

For chart input JSON rather than raw SVG where possible.

This keeps tests stable when presentation changes.

## Browser-level checks

At minimum verify:

- no JavaScript errors;
- dark/light mode;
- chart resize;
- hash navigation;
- empty data;
- partial data;
- sampled data;
- large application limits.

## Offline test

A generated Explorer should be opened with network access blocked.

After the D3 migration is complete, **all functionality should remain available**.

---

# 29. Success criteria

The redesign is successful if a user can answer these questions without manually combining multiple tables:

- What are the 3 stages I should investigate first?
- Is the run CPU-bound, memory-bound, shuffle-bound or scheduler-bound?
- Is there task skew?
- Is one executor or node unhealthy?
- Were executor slots sitting idle?
- Did dynamic allocation react too slowly?
- Which stages actually controlled wall-clock runtime?
- Which SQL query caused those stages?
- What was happening elsewhere in the cluster at the same time?
- Is the evidence complete or sampled?

---

# 30. Recommended first implementation PR

The best first implementation PR after this design proposal would contain:

1. **Stage Health Map**
2. **Task Time Breakdown**
3. **Task Distribution Comparison**
4. links from those charts into existing Explorer stage detail pages

Why these first:

- they reuse data already present;
- they require relatively little architecture change;
- they improve the report immediately;
- they establish reusable visual primitives;
- they provide much more diagnostic value than simply adding another metric table.

The **Stage × Executor Heatmap** should follow immediately after, because it is likely to become one of the Explorer's strongest investigation tools.
