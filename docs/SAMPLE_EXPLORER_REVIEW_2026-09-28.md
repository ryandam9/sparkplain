# Sample Explorer UI/visualisation review

**Date:** 28 September 2026  
**Reviewed sample:** `application_1790502368481_0002-explorer.html`  
**Application:** `gdelt_country_pairs`  
**Generator in sample:** `sparkplain e854c84`  
**Scope:** Explorer information architecture, D3 visualisations, drill-down flow, chart prioritisation, and next visual improvements

## Executive summary

The sample Explorer is much further along than an earlier source-level review suggested.

It already contains several of the visualisations that would normally be proposed as future work:

- a D3-backed chart kit;
- a Stage × Executor heatmap;
- a unified run timeline;
- stage-time decomposition;
- per-stage task-duration distributions;
- task scatter plots;
- executor lifetime/time/heap views;
- job and stage DAGs;
- an application anatomy diagram;
- interactive hash-based drill-down into jobs, stages, executors, queries, code and evidence.

That changes the recommendation.

The priority should **not** be to keep adding generic charts. The Explorer already has enough chart types. The best next work is to make the most diagnostic views easier to discover, remove duplicated visual emphasis, and add only the few application-wide views that are still missing.

Recommended next priorities:

1. **Stage Health Map**
2. **Critical Path DAG**
3. **Cross-stage task-skew / percentile chart**
4. **Overview redesign**
5. **Selectable axes/metrics for the existing task scatter**
6. **Move the Stage × Executor heatmap higher in the Executors page**
7. **Reduce primary navigation clutter**

The current Stage detail experience is already strong and should not receive many more charts until the application-level navigation and correlation views improve.

---

# 1. Overall assessment

## What is working well

The Explorer has a clear engineering-tool visual language rather than a generic dashboard appearance.

Strong aspects include:

- IBM Plex Sans / Condensed / Mono typography;
- restrained neutral surfaces;
- semantic success/warning/critical colours;
- dark-mode support;
- sticky page navigation;
- sticky table headers and first columns;
- local scrolling for large tables;
- compact chart framing;
- captions and "How to read" guidance;
- keyboard/focus styling;
- sample/partial-data labelling;
- chart drill-down links;
- consistent use of application theme variables by D3.

The design feels suitable for a Spark diagnostic tool.

The sample also demonstrates that Sparkplain is already trying to explain the meaning of charts rather than merely presenting metrics. That should remain a product principle.

## Main opportunity

The main weakness is now **information hierarchy**, not chart capability.

There are many useful visualisations, but several important diagnostic questions require the user to know where to navigate first.

The Explorer should increasingly optimise for:

> What should I investigate next?

rather than:

> What metrics are available?

---

# 2. Important correction to the earlier roadmap

Several previously proposed features are already implemented and should be removed from the "future work" list.

## Already implemented: Stage × Executor heatmap

The sample contains a dedicated Stage × Executor heatmap.

It already supports the important interaction pattern:

- executor rows;
- stage columns;
- metric-driven cells;
- large-matrix limits;
- D3 rendering;
- clickable cells that can carry the user toward relevant stage/executor context.

This is a high-value feature and should be promoted rather than rebuilt.

### Recommendation

Keep the implementation and improve placement/discoverability.

Do **not** create a second competing executor-stage chart.

---

## Already implemented: Unified run timeline

The sample contains a combined run timeline rather than only independent time-series charts.

It aligns application activity such as:

- SQL/query work;
- jobs;
- stages;
- executors;
- running tasks;
- CPU;
- container pressure / pending containers.

This is exactly the right direction for diagnosing correlation.

### Recommendation

Keep it as the canonical time-correlation view.

Avoid creating more independent timeline charts in Overview when the dedicated Timeline page already provides the richer experience.

---

## Already implemented: Stage-time decomposition

The sample already visualises where stage/task time went using categories such as:

- starting / scheduler overhead;
- computation;
- garbage collection;
- shuffle;
- result handling;
- other time.

This already answers the "why is this stage slow?" question much better than a simple duration ranking.

### Recommendation

Keep this chart.

If anything, improve comparison and filtering rather than creating another time-breakdown visual.

---

# 3. The Overview page should change the most

The Overview is the highest-value page because it determines what the user notices first.

At present, the broad flow is approximately:

1. What happened
2. KPIs
3. Over time
4. Findings
5. Critical path
6. Longest stages

This gives time-series information too much prominence before the user has seen the diagnosis.

## Recommended Overview hierarchy

Reorder the page to:

1. **What happened**
2. **KPIs**
3. **Findings**
4. **Stage Health Map**
5. **Critical Path**
6. **Resource utilisation**
7. **Compact unified timeline**
8. **Top stages / supporting table**

This creates a more diagnostic progression:

> summary → problem → location → dependency → resources → chronology

rather than:

> summary → chronology → diagnosis

## Why Findings should move higher

In the reviewed sample, Sparkplain already identified several useful findings, including:

- Spark requested far more executors than the cluster could place;
- executor heap was substantially under-used;
- JVM CPU share was low;
- some input tasks ran away from their data.

Those findings are more actionable than seeing three timeline charts first.

The user should encounter them immediately after the application headline figures.

---

# 4. Add a Stage Health Map

This is the strongest missing application-wide chart.

The current Stages page provides multiple separate views:

- longest stages;
- stage data moved;
- spill;
- stage-time breakdown;
- a stage table.

Those are individually useful, but they require mental correlation.

## Proposed D3 chart

Each stage is one bubble.

### X axis

**Stage wall-clock duration**

### Y axis

**Total data moved**

Recommended definition:

`input + shuffle read + shuffle write + output`

Disk spill can remain a separate severity dimension rather than being folded into "data moved".

### Bubble size

**Task count**

### Bubble styling

Use stroke / icon / badge to indicate one or more of:

- failed;
- retried;
- severe skew;
- significant spill;
- high GC;
- critical-path membership.

Do not encode every dimension by colour.

### Tooltip

Show:

- stage ID and name;
- wall time;
- task count;
- input;
- shuffle read;
- shuffle write;
- output;
- spill;
- CPU share;
- GC share;
- median task time;
- p95 task time;
- max task time;
- max / median ratio;
- critical-path status.

### Click

Navigate directly to:

`#stage/<stage-key>`

## Why this is valuable

It lets the user distinguish:

### Long duration + large data

Probably genuinely expensive work.

### Long duration + low data

Potentially more interesting:

- scheduler delay;
- GC;
- Python/UDF work;
- external waits;
- insufficient parallelism;
- skew.

### Large task count + short duration

Good parallelism.

### Small task count + long duration

Possible under-partitioning or serial work.

### Significant spill

Potential memory/partition sizing issue.

## Placement

### Overview

Compact version showing the most important stages.

### Stages

Full version with interaction and filters.

---

# 5. Replace the text-like Critical Path with a D3 DAG

The current Explorer has sophisticated D3 visualisations elsewhere, but the Critical Path presentation is comparatively lightweight.

It should become a first-class visual.

## Proposed graph

Render the stage dependency DAG using the same visual vocabulary as the existing job/stage DAGs.

### Nodes

Stage nodes show:

- Stage ID
- duration
- status

Optional second line:

- tasks or data moved

### Critical path

Use:

- thicker edges;
- stronger stroke;
- stronger node border.

### Non-critical branches

Use muted strokes and lighter nodes.

### Warning state

Use a warning/critical border for:

- failure;
- severe skew;
- high spill;
- high GC.

### Tooltip

Show:

- duration;
- tasks;
- data moved;
- CPU;
- GC;
- skew ratio;
- spill;
- parent/child count.

### Click

Open the stage detail.

## Key explanatory text

The chart should explicitly say:

> The critical path is the dependency chain constraining application completion. A long stage outside the critical path can be expensive without increasing wall-clock runtime.

This distinction is important.

## Reuse existing DAG infrastructure

The Explorer already has DAG rendering for jobs/stages.

The Critical Path view should reuse:

- node layout conventions;
- edge styling;
- hover/focus behaviour;
- click navigation;
- theme variables.

Avoid introducing a visually unrelated graph renderer.

---

# 6. Add a cross-stage Task Skew / Percentile chart

The Stage detail view is already strong.

It includes task distribution values such as:

- minimum;
- p25;
- median;
- p75;
- maximum;
- totals;
- individual slow tasks;
- a histogram;
- a task scatter plot.

The missing question is application-wide:

> Which stage is skewed?

Today the user must inspect stages individually.

## Proposed chart

A horizontal percentile-range / box-and-whisker-style D3 chart.

One row per stage.

Show:

- min;
- p25;
- median;
- p75;
- p95;
- max.

## Default sort

Sort descending by:

`p95 / median`

or:

`max / median`

Prefer `p95 / median` as the main robust skew signal, with max/median shown secondarily.

## Row annotation

Show:

- task count;
- p95/median ratio;
- max/median ratio;
- speculative tasks;
- failed tasks.

## Highlight rules

Potential visual thresholds:

- p95 >= 2× median: mild;
- p95 >= 3× median: warning;
- p95 >= 5× median: severe.

These should be treated as explanatory heuristics, not universal Spark correctness rules.

## Placement

Add to **Stages**.

Do not add it separately to each Stage detail because that page already has sufficient distribution visualisation.

---

# 7. Avoid stacking six charts on the Stages page

If Stage Health Map and cross-stage skew are added, the Stages page could become visually overloaded.

Existing views already include several chart types.

## Recommended approach

Use one primary chart area with a selector.

Example:

`View: Health | Duration | Data | Time breakdown | Skew | Spill`

Render one major analytical chart at a time.

Keep the stage table below it.

## Suggested default

**Health**

because it combines several dimensions and best answers:

> Which stages deserve attention?

## Benefits

- less vertical scrolling;
- easier comparison;
- clearer page purpose;
- fewer competing visual elements;
- better mobile behaviour;
- easier future chart additions without page growth.

---

# 8. Promote the Stage × Executor heatmap

The heatmap is one of the strongest features in the Explorer.

It should appear earlier on the Executors page.

## Recommended order

1. **Stage × Executor heatmap**
2. Executor lifetimes
3. Executor time breakdown
4. Heap utilisation
5. Executor table

## Why

The heatmap answers the broad diagnostic question:

> Is one executor behaving differently from its peers?

The following charts then explain that executor.

Today the user may encounter executor-specific charts before seeing the cross-stage comparison that would tell them which executor to investigate.

---

# 9. Do not add many more Stage detail charts

The current Stage detail page is already dense and useful.

It contains enough information to support deep investigation:

- status;
- duration;
- attempts;
- partitions;
- CPU share;
- jobs;
- parent stages;
- locality;
- shuffle/cache information;
- DAG;
- source/code linkage;
- task percentile table;
- task-time breakdown;
- duration histogram;
- task scatter;
- slowest tasks;
- sample tasks;
- executor breakdown.

This is already a very capable diagnostic page.

## Recommendation

Improve **interaction** rather than adding more independent charts.

---

# 10. Make the existing task scatter configurable

The task scatter is one of the most extensible existing visualisations.

Instead of adding many more scatter charts, turn it into a small correlation explorer.

## Default view

Keep:

- X = task start time
- Y = duration

This is excellent for waves and stragglers.

## Suggested X-axis options

- start time;
- input bytes;
- input records;
- shuffle read;
- partition index.

## Suggested Y-axis options

- duration;
- input bytes;
- input records;
- shuffle read;
- shuffle fetch wait;
- GC time;
- scheduler delay;
- result size;
- peak execution memory, when available.

## High-value combinations

### Input bytes → duration

Helps distinguish data skew from infrastructure slowness.

### Records → duration

Useful when row sizes are relatively consistent.

### Shuffle read → fetch wait

Useful for shuffle/network bottlenecks.

### Peak execution memory → GC

Useful for memory pressure.

### Start time → duration

Keep as default for detecting waves/stragglers.

## Sampling

Preserve the sample disclosure already present in the Explorer.

The plot must clearly indicate whether it contains:

- every task;
- sampled tasks;
- sample + slowest tasks.

---

# 11. Keep "How to read" guidance

This is one of Sparkplain's best design choices.

The Explorer has explicit styling for:

- chart captions;
- "How to read" guidance;
- explanatory text.

Keep that pattern for every new visual.

## Every chart should answer

### What does this show?

For example:

> Stage wall time plotted against the amount of data each stage moved.

### How do I read it?

For example:

> Stages far to the right but low on data are candidates for scheduler, skew, GC or Python-related investigation.

### Is the data complete?

Say whether:

- all tasks are included;
- tasks are sampled;
- stage × executor cells were capped;
- CloudWatch data was unavailable;
- only partial logs were read.

### What happens when I click?

Make drill-down discoverable.

---

# 12. Navigation is near its practical limit

The Explorer can expose many tabs, including areas such as:

- Overview
- At a glance
- Timeline
- Jobs
- Stages
- Executors
- SQL/DataFrame
- Storage
- Code
- Environment
- Event log
- Cluster
- Logs

This is comprehensive, but the primary tab row risks becoming too crowded.

## Recommended primary navigation

- Overview
- Timeline
- Jobs
- Stages
- SQL
- Executors
- Cluster

Then group lower-frequency views under **More**:

- Storage
- Code
- Environment
- Event log
- Logs

## At a glance

Consider moving **At a glance** into Overview rather than retaining a top-level tab.

The anatomy diagram is useful, but it is naturally part of the application overview.

---

# 13. Keep the application anatomy diagram

The anatomy diagram is a distinctive Sparkplain visual.

It communicates:

- EMR nodes;
- YARN capacity;
- driver/executor placement;
- executor container size;
- heap usage;
- node CPU;
- resource gaps;
- finding badges.

This is richer than a generic infrastructure table.

## Recommendation

Do not replace it with a generic chart.

Continue improving:

- zoom/pan;
- finding highlighting;
- click-through;
- responsive behaviour;
- large-cluster simplification.

For very large clusters, consider grouping similar worker nodes rather than attempting to draw every node at equal detail.

---

# 14. Resource utilisation should be compact, not another dashboard

The sample has enough data to tell an important story:

- large executor request;
- limited executor placement;
- very low JVM CPU share;
- substantial unused configured heap.

A compact resource-utilisation panel on Overview would help correlate these findings.

## Recommended metrics

- task-slot utilisation;
- JVM CPU share;
- peak heap / configured heap;
- YARN memory occupancy;
- pending container time;
- executor losses;
- disk spill.

Use horizontal utilisation bars or compact bullet-style charts.

## Avoid

Do not calculate one opaque "Spark health score".

Individual measures are more transparent and defensible.

---

# 15. Use the reviewed sample as a visual regression fixture

The sample application is a particularly useful design fixture because it contains multiple interesting conditions:

- the application succeeds;
- Spark requested far more executors than the cluster could place;
- executor heap is under-used;
- JVM CPU share is low;
- task locality has a notable finding;
- many jobs/stages/tasks are present;
- multiple executors and hosts are present;
- CloudWatch/node context is present.

This gives new charts meaningful data to display.

## Recommended fixture use

Use this application when visually testing:

- Stage Health Map;
- Critical Path DAG;
- skew chart;
- heatmap;
- unified timeline;
- executor resource charts;
- dark mode;
- large tables;
- drill-down navigation.

Where possible, keep a deterministic generated fixture under test data or snapshot infrastructure rather than relying on a manually preserved standalone HTML file.

---

# 16. D3 component reuse

The Explorer already has a D3 chart kit.

New charts should reuse shared primitives rather than introducing one-off chart code.

Recommended reusable components:

- chart frame;
- axes;
- time scale;
- tooltip;
- legend;
- drill-down target;
- keyboard-focusable mark;
- empty state;
- partial-data state;
- scatter renderer;
- percentile-range renderer;
- heatmap renderer;
- DAG renderer;
- synchronized cursor.

## New primitives needed

For the proposed next work:

### Stage Health Map

Reuse scatter plot primitives plus variable bubble radius.

### Critical Path

Reuse DAG primitives.

### Task skew chart

Add a percentile-range primitive.

This keeps the implementation small.

---

# 17. Colour semantics

The current sample has good semantic separation.

Continue the rule:

- critical/failure colours = status only;
- warning colours = warning only;
- neutral = context/unknown;
- categorical palette = series;
- accent = interaction/selection.

For Stage Health Map, avoid encoding all health dimensions by colour.

A useful scheme:

- normal bubble fill = categorical/neutral series;
- critical-path = stronger outline;
- failed = critical outline;
- skew = small warning marker;
- spill = hatch/badge/secondary marker.

This improves accessibility and avoids turning the chart into a rainbow.

---

# 18. No new pie/donut charts

No additional pie or donut charts are recommended.

They would not improve the major diagnostic questions in this Explorer.

Prefer:

- bars for comparisons;
- stacked bars for composition;
- line/area for ordered time series;
- scatter for relationships;
- heatmap for two-dimensional comparison;
- percentile ranges for distributions;
- DAGs for dependencies.

---

# 19. Responsive behaviour

The sample already uses horizontal scrolling for visualisations that should not shrink below a readable size.

That is appropriate for:

- timelines;
- heatmaps;
- anatomy;
- DAGs.

Continue this approach rather than compressing labels until unreadable.

## For Stage Health Map

On narrow screens:

- retain a minimum plot width;
- allow horizontal scrolling;
- keep tooltip/selection tap-friendly;
- consider a compact stage list underneath.

## For task skew

Horizontal percentile rows are naturally mobile-friendly if labels remain short.

---

# 20. Accessibility

Keep the existing focus-visible and semantic text patterns.

For new D3 marks:

- use `tabindex=0` on actionable nodes/points;
- support Enter/Space navigation;
- provide `aria-label`;
- do not rely on hover;
- make click/tap selection persistent;
- ensure warning/failure is not colour-only.

For a Stage Health bubble:

> Stage 42, 3 minutes 46 seconds, 14.2 GiB moved, 640 tasks, high skew, on critical path.

would be a good accessible label.

---

# 21. Performance constraints

The Explorer operates on potentially large applications.

New visuals must keep explicit rendering budgets.

## Stage Health Map

If there are thousands of stages:

- default to the most relevant 100–200;
- allow filters;
- indicate truncation;
- retain failed/critical-path stages even if they are outside the top-N.

## Critical Path DAG

Render:

- critical chain;
- immediate parallel branches;
- optionally expand the full DAG.

Do not draw an unreadable thousand-node DAG by default.

## Task skew chart

Show:

- top 30–50 stages by skew or duration;
- selectable sort/filter;
- "show more" where practical.

---

# 22. Proposed Overview mock structure

Recommended hierarchy:

```text
APPLICATION HEADER
gdelt_country_pairs · succeeded · 5m 14s

WHAT HAPPENED
Plain-language summary

KPI STRIP
Duration | Executors | CPU | Heap | Input | Shuffle | Findings

FINDINGS
Warning / info cards

STAGE HEALTH
Bubble scatter

CRITICAL PATH
D3 dependency graph

RESOURCE UTILISATION
CPU | slots | heap | YARN

RUN TIMELINE
Compact version / link to full Timeline

TOP STAGES
Small ranked table
```

This would make Overview a diagnostic landing page rather than a miniature version of every other tab.

---

# 23. Proposed Stages page

Recommended layout:

```text
STAGES

View:
[ Health ] [ Duration ] [ Data ] [ Time ] [ Skew ] [ Spill ]

<one main D3 chart>

STAGE TABLE
sortable/filterable
```

Clicking a chart mark and clicking a table row should navigate to the same Stage detail.

---

# 24. Proposed Executors page

Recommended order:

```text
EXECUTORS

Stage × Executor Heatmap

Executor lifetimes

Where executor time went

Peak heap

Executor table
```

This puts the broad comparison first and the per-executor explanations second.

---

# 25. Stage detail: recommended change only

Do not add several new charts.

Add controls to the existing task scatter:

```text
X axis: Start time ▼
Y axis: Duration ▼
```

Potential optional control:

```text
Colour: Status ▼
```

Useful colour choices:

- status;
- executor;
- locality;
- attempt.

Do not use all of them simultaneously.

---

# 26. Suggested implementation phases

## Phase 1 — highest value

### A. Stage Health Map

Add:

- reusable bubble scatter;
- Overview compact view;
- Stages full view;
- click-to-stage navigation.

### B. Cross-stage skew chart

Add:

- percentile-range renderer;
- p95/median sort;
- click-to-stage navigation.

These two charts answer application-wide questions currently missing.

## Phase 2 — dependency story

### C. Critical Path DAG

Reuse the existing DAG infrastructure.

Add:

- critical-path emphasis;
- warning badges;
- click-to-stage;
- explanatory text.

## Phase 3 — UX hierarchy

### D. Overview restructuring

Move Findings higher.

Reduce duplicated "Over time" content.

Add compact resource utilisation.

### E. Executors restructuring

Move heatmap to the top.

## Phase 4 — interaction

### F. Configurable task scatter

Add X/Y metric selectors.

Reuse the existing plot instead of creating multiple new scatter charts.

## Phase 5 — navigation

### G. Primary / More navigation grouping

Reduce the number of always-visible top-level tabs.

---

# 27. What not to do next

The sample is already chart-rich.

Avoid these next moves:

- another general CPU line chart;
- another executor-count chart;
- separate scatter plots for every task metric;
- multiple new pie charts;
- another independent job timeline;
- a duplicate heatmap;
- adding many more charts to Stage detail;
- a single synthetic "health score";
- creating new chart frameworks alongside the existing D3 kit.

These would add visual volume without materially improving diagnosis.

---

# 28. Recommended priority table

| Priority | Change | Surface | Why |
|---|---|---|---|
| P0 | Stage Health Map | Overview + Stages | Best missing application-wide diagnostic |
| P0 | Cross-stage skew chart | Stages | Finds straggler stages without opening each one |
| P1 | Critical Path DAG | Overview | Shows what actually constrained completion |
| P1 | Overview redesign | Overview | Puts diagnosis before chronology |
| P1 | Move executor heatmap higher | Executors | Strong existing chart deserves prominence |
| P1 | Selectable task scatter metrics | Stage detail | Adds correlation power without chart clutter |
| P2 | Primary/More tab grouping | Explorer shell | Reduces navigation overload |
| P2 | Compact resource utilisation | Overview | Correlates capacity findings |
| P3 | Large-DAG/stage scalability modes | Explorer | Keeps charts usable on huge applications |

---

# 29. Acceptance criteria for the next implementation

## Stage Health Map

- renders without external network dependencies;
- uses current theme tokens;
- keyboard accessible;
- click opens stage;
- supports partial data;
- clearly states metrics/units;
- preserves failed and critical stages under top-N limits;
- does not imply that "high data = bad".

## Cross-stage skew

- displays p25/median/p75/p95/max or equivalent;
- sorts by skew ratio;
- labels sampling/partial status;
- click opens stage;
- handles stages with zero/insufficient successful tasks.

## Critical Path DAG

- critical chain visibly distinct;
- non-critical branches remain visible but secondary;
- handles failed/incomplete stages;
- click opens stage;
- large DAG falls back to a bounded focused view.

## Overview

A user should be able to identify:

1. the main findings;
2. the stages worth investigating;
3. the critical dependency chain;
4. whether resource usage looks constrained;
5. when the important events occurred;

without opening another tab.

---

# 30. Final recommendation

The sample Explorer is already a strong diagnostic UI.

The next step should be **curation and correlation**, not chart proliferation.

The three visual changes with the highest expected value are:

1. **Stage Health Map**
2. **Cross-stage task-skew chart**
3. **Critical Path DAG**

After those, spend effort on:

- Overview hierarchy;
- heatmap placement;
- task scatter configurability;
- navigation simplification.

The existing unified timeline, Stage × Executor heatmap, stage-time decomposition, anatomy view, task histogram/scatter and drill-down routes should be treated as existing strengths and evolved rather than replaced.
