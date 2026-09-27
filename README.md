# sparkdq-medicare — PySpark Data-Quality Checker for Medicare Part A/B

A small, declarative data-quality engine for PySpark, with built-in check
profiles for the **CMS Medicare Part A/B public dataset**
(DE-SynPUF 2008–2010, Sample 1).

Medicare Part A covers inpatient hospital stays, skilled nursing and hospice
care; Part B covers doctors' services, outpatient care and durable medical
equipment. The public files are:

| File | Contents |
|---|---|
| Beneficiary Summary (per year) | demographics, coverage months, chronic-condition flags, annual reimbursements |
| Inpatient Claims | Part-A-type institutional claims |
| Outpatient Claims | Part-B-type institutional claims |
| Carrier Claims (Part A / Part B) | physician/supplier claims, 13 line items per row |

Every file links on `DESYNPUF_ID` (synthetic beneficiary id). Column names
follow the official CMS DE-SynPUF codebook.

## Quickstart

Prerequisites: Python 3.9+ and a Java 8+ runtime (`java -version`) — Spark
needs a JVM even in local mode.

```bash
pip install -r requirements.txt        # pyspark, pytest
pip install -e .                       # install sparkdq

# 1. create the synthetic demo data (or download the real data, see data/README.md)
python examples/generate_sample_data.py

# 2. run all Medicare DQ suites
python examples/run_medicare_checks.py
# reports -> reports/*.md and reports/*.json
```

> **Restricted containers:** the example and test session set
> `spark.python.use.daemon=false` because Spark's forking Python daemon exits
> immediately when its stdin is at EOF (typical in locked-down CI/sandbox
> runners), surfacing as `Python daemon failed to launch worker`. On a normal
> machine or cluster you can drop that setting — plain workers are just a
> little slower per task.

## What it checks

**Engine (reusable for any DataFrame)** — `src/sparkdq/`:

| Check | What it does |
|---|---|
| `row_count` | table is not empty |
| `not_null` / `unique` | column completeness & key integrity |
| `accepted_values` | categorical domains (e.g. sex/race codes) |
| `numeric_range` | value ranges (e.g. payments ≥ 0) |
| `regex_match` | string patterns |
| `date_order` | `start <= end` (claim dates, birth/death) |
| `referential_integrity` | orphan detection via anti-join (claims → beneficiary) |
| `schema_match` | columns & types vs expected schema |
| `custom_sql` | any SQL predicate as a check |

Each check carries a severity (`error`/`warning`) and returns violating-row
samples. Reports render as Markdown and JSON.

**Medicare profiles** — `src/sparkdq/profiles/medicare.py`:
`beneficiary`, `inpatient`, `outpatient`, `carrier` suites with the real
codebook value domains (sex 1/2, race 1–5, chronic flags Y/N, coverage
months 0–12), date-order rules, and `DESYNPUF_ID` referential integrity
from every claim file back to the beneficiary file.

```python
from pyspark.sql import SparkSession
from sparkdq import DQEngine
from sparkdq.profiles import medicare

spark = SparkSession.builder.master("local[*]").getOrCreate()
bene = medicare.preprocess(spark.read.csv("beneficiary.csv", header=True))
claims = medicare.preprocess(spark.read.csv("inpatient.csv", header=True))

report = DQEngine(spark).run(
    claims, medicare.checks_for("inpatient"), table_name="inpatient",
    ref_tables={"beneficiary": bene})
print(report.to_markdown())
```

## Sample output

Running the demo on the synthetic data (`examples/run_medicare_checks.py`)
produces reports like this — the injected issues are caught:

```markdown
# Data Quality Report — `inpatient`

**Overall: FAIL** · 8 passed · 3 failed · 1 warning(s)

| Check | Column | Severity | Status | Violations | Details |
|---|---|---|---|---|---|
| CLM_ID_unique | CLM_ID | error | ❌ fail | 2 (20.0%) | 2 row(s) with duplicated CLM_ID |
| CLM_FROM_DT_before_CLM_THRU_DT | - | error | ❌ fail | 1 (10.0%) | 1 row(s) where CLM_FROM_DT > CLM_THRU_DT |
| CLM_PMT_AMT_in_range | CLM_PMT_AMT | warning | ❌ fail | 1 (10.0%) | 1 row(s) with CLM_PMT_AMT outside [0, None] |
| claim_beneficiary_link | DESYNPUF_ID | error | ❌ fail | 1 (10.0%) | 1 orphan row(s): DESYNPUF_ID not found in parent.DESYNPUF_ID |
```

## Tests

```bash
pytest tests/ -q
```

## Project layout

```
src/sparkdq/
  checks.py            check definitions (dataclasses + builders)
  engine.py            DQEngine: executes checks on DataFrames
  report.py            DQReport: markdown/JSON rendering
  profiles/medicare.py DE-SynPUF schemas, preprocessing, check suites
examples/
  generate_sample_data.py  synthetic demo CSVs (NOT real Medicare data)
  run_medicare_checks.py   end-to-end runner
tests/                 pytest suite (engine + Medicare profiles)
data/README.md         how to download the real CMS dataset
```

## Notes

- The bundled `examples/sample/*.csv` files are **synthetic demo data** with
  deliberately injected quality issues (duplicates, bad codes, orphan rows)
  so the example report shows real findings.
- Payment-amount checks are `warning` severity: the synthetic generator can
  legitimately produce negative adjustments.
