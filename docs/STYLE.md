# Writing style: ASD-STE100 on the report and explorer

The text on the report and explorer pages follows ASD-STE100 Simplified Technical English (STE), where it applies. STE is a controlled language for technical text. It limits the words, their meanings and the sentence shapes, so that every reader gets one meaning, including readers whose first language is not English.

This page says how sparkplain applies STE. `TestPlainLanguage` (in `internal/sparkplain/report`) checks the rules that a program can check.

## What the rules cover

They cover all text that sparkplain writes for people: findings (title, problem, "Try"), the summary, metric explanations, coverage and Sources text, chart guides, "In this run" notes, page introductions, labels and tooltips. The same text goes into the JSON's text fields and the console's findings, so they change too.

They do not cover text that sparkplain quotes or names:

- Log lines and error messages, as Spark, YARN, HBase or AWS wrote them.
- Spark's own names, such as stage names (`collect at job.py:32`) and plan operators.
- Configuration keys and values, paths, host names, user names and IDs.

## Rules

1. **Sentence length.** An instruction has 20 words or fewer. A description has 25 words or fewer. Numbers, units and names count as words.
2. **One topic in each sentence.** Do not join two statements with a semicolon or a colon. Write two sentences, or a list.
3. **One instruction in each sentence.** Start an instruction with a verb: "Set", "Make sure", "Look at", "Remove". Put a sequence of instructions in a numbered list, and alternatives in a bulleted list.
4. **Paragraphs.** A paragraph has six sentences or fewer.
5. **Tenses.** Use the simple present, the simple past and the simple future. Do not use the perfect tenses ("has been", "had run").
6. **Active voice.** Say who or what does the action: "Spark removed the executor", not "the executor was removed". Use the passive only when the agent is not known.
7. **No contractions.** Write "do not", "cannot", "it is".
8. **No -ing words as nouns.** Write "when Spark reads the data", not "reading the data".
9. **Noun clusters.** Do not put more than three nouns in a row. Break longer ones with "of", "for" or a relative clause.
10. **Articles.** Use "the", "a" and "this" where English permits them.
11. **One word, one meaning.** Use each approved word only in its approved meaning. Use each technical name for one thing only, everywhere.
12. **Lists.** Use a vertical list for three or more items.

## Words

STE approves a set of general words, each with one meaning. It also permits technical names and technical verbs for the subject. The official dictionary is in ASD-STE100, which ASD gives free of charge after registration. It is licensed, so it is not in this repository.

### Words the check refuses

These words are not approved in STE. `TestPlainLanguage` refuses them and gives the word to use instead:

| Do not write | Write |
| --- | --- |
| accomplish, perform | do |
| additional | more |
| adjacent | next to |
| allow | let |
| approximately | about |
| assist, facilitate | help |
| commence, initiate | start |
| consequently | as a result |
| demonstrate, indicate | show |
| eliminate | remove |
| employ, utilize | use |
| enable | let, or (for a setting) set to true |
| ensure | make sure |
| exceed | be more than |
| happen | occur |
| however | but |
| insufficient | not sufficient |
| numerous | many |
| obtain | get |
| prior to | before |
| provide | give |
| require | be necessary |
| whether | if |
| may, might | can |
| via | through |
| upon | on |
| a lot, lots | many, much |
| e.g., i.e. | for example, that is |

### Words to confirm against the dictionary

We think that STE does not approve these words, or approves them only in a different meaning. They are not in the check until someone confirms them against the dictionary:

| Word | Probable approved form |
| --- | --- |
| need (verb) | necessary ("the event log is necessary") |
| check (verb) | make sure, examine |
| fix (verb) | correct, repair |
| see (meaning "refer to") | refer to |
| since (meaning "because") | because |
| although, though | but |
| various | different |
| therefore | thus? |

### Technical names

These are technical names in this subject. STE permits them:

- **Spark:** application, application attempt, driver, executor, task, task attempt, stage, job, partition, shuffle, broadcast variable, accumulator, heap, off-heap, garbage collection, spill, cache, storage memory, execution memory, scheduler delay, straggler, call site, event log, query, plan, adaptive execution (AQE), critical path, slot.
- **YARN and Hadoop:** YARN, ResourceManager, NodeManager, container, queue, vCore, application master, HDFS, Kerberos, principal, ticket.
- **EMR and AWS:** cluster, primary node, core node, task node, instance, instance type, instance profile, spot, on-demand, step, bootstrap action, S3, EMRFS, CloudWatch, CloudTrail, IAM, role, security configuration.
- **HBase:** table, region, region server, row key, scan, split, memstore, flush, compaction, ZooKeeper, quorum.
- **Units:** vCPU, core, GiB, MiB, KiB, ms, s.

"What happened" stays as the name of the report's first section, as CLAUDE.md asks, although "happen" is not an approved word.

## The check

`TestPlainLanguage` reads every string that can reach the pages:

- the Go strings of the `analyze`, `model` and `report` packages (a `Fix` field's strings are instructions);
- the text of `templates/report.html.tmpl`;
- the strings of `assets/explorer.js`.

It refuses a sentence that is too long, a contraction, and a word from the table above. It cannot check meaning, tense, voice or noun clusters. A person must review those.

Text that broke a rule before the check came in is listed in `internal/sparkplain/report/testdata/ste-baseline.txt`. That list only shrinks. A new break fails the test, and so does a listed entry that is no longer in the text. After you rewrite text, refresh the list:

```sh
go test ./internal/sparkplain/report -run TestPlainLanguage -ste-update
```

A string that is not prose (a log pattern or a code fragment) can be skipped with a `ste:ignore` comment on its line.
