"""Run the sparkdq Medicare Part A/B data-quality suites.

Usage:
    python examples/generate_sample_data.py   # first time: create demo CSVs
    python examples/run_medicare_checks.py [--data DIR] [--out DIR]

To check the REAL CMS DE-SynPUF files instead, point --data at the folder
with the downloaded CSVs and name them:
    beneficiary.csv  inpatient.csv  outpatient.csv  carrier_b.csv
(see data/README.md for download instructions)
"""

import argparse
import os
import sys

# Workers must use this interpreter (it has pyspark installed).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession

from sparkdq import DQEngine
from sparkdq.profiles import medicare


def load(spark, path):
    return spark.read.option("header", "true") \
        .option("inferSchema", "false").csv(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(os.path.dirname(__file__), "sample"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "reports"))
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("medicare-dq")
             .master("local[2]")
             # Skip the forking Python daemon so the demo also runs in
             # locked-down containers (see tests/conftest.py for why).
             .config("spark.python.use.daemon", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    engine = DQEngine(spark)
    tables = {}
    for name in ("beneficiary", "inpatient", "outpatient", "carrier"):
        fname = "carrier_b.csv" if name == "carrier" else f"{name}.csv"
        path = os.path.join(args.data, fname)
        if not os.path.exists(path):
            print(f"!! skipping {name}: {path} not found")
            continue
        tables[name] = medicare.preprocess(load(spark, path))

    os.makedirs(args.out, exist_ok=True)
    overall = []
    for name, df in tables.items():
        checks = medicare.checks_for(name)
        ref = {"beneficiary": tables["beneficiary"]} if "beneficiary" in tables else {}
        report = engine.run(df, checks, table_name=name, ref_tables=ref)
        print(report.to_markdown())
        report.save_markdown(os.path.join(args.out, f"{name}.md"))
        report.save_json(os.path.join(args.out, f"{name}.json"))
        overall.append((name, report.overall_status))

    print("=" * 60)
    for name, status in overall:
        print(f"{name:>12}: {status}")
    print(f"Reports written to {args.out}/")
    spark.stop()


if __name__ == "__main__":
    main()
