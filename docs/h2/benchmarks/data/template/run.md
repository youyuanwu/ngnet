# NN — <what question this run was asked>

<!-- Copy to data/<machine-id>/NN-<slug>.md and fill in. The header is not ceremony: a table
     of numbers without it cannot be compared with anything later. -->

**Machine:** [`<machine-id>`](README.md)
**Date:** YYYY-MM-DD
**Commit(s):** baseline `<sha>` against `<sha>` — or a single sha for a survey
**Cases:** which bench targets were run
**Command:** the exact invocation, including `taskset`
**Repetitions:** how many per side, and interleaved or not
**Controls:** which arms were unchanged, and how far they moved
**Exclusions:** the rule, fixed before the numbers were seen, and how many replicates it took

## What was being asked

One paragraph. A run that cannot state its question in one paragraph was not a controlled
measurement.

## Results

<!-- Tables. Say what the units are and which direction is better. Where a comparison is
     paired, report the paired delta rather than two absolutes. -->

| Measure | arm | arm | arm |
| --- | --- | --- | --- |
| | | | |

## Drift controls in the same session

<!-- The unchanged arms and their movement. A result smaller than this is not a result. -->

| Control arm | Movement |
| --- | --- |
| | |

## What this establishes

- What the run supports, stated so that it could be falsified.

## What it does not

- Which arms, sizes or configurations were **not** swept, and which questions therefore remain
  open. This section is the one most often skipped and most often needed later.
