"""End-to-end test: run the Medicare profiles against the synthetic demo data."""

import os
import subprocess
import sys

from sparkdq import DQEngine
from sparkdq.profiles import medicare

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "examples", "sample")


def _ensure_sample_data():
    if not os.path.exists(os.path.join(SAMPLE, "beneficiary.csv")):
        gen = os.path.join(os.path.dirname(__file__), "..", "examples",
                           "generate_sample_data.py")
        subprocess.run([sys.executable, gen], check=True)


def _load(spark, name):
    _ensure_sample_data()
    fname = "carrier_b.csv" if name == "carrier" else f"{name}.csv"
    df = (spark.read.option("header", "true").option("inferSchema", "false")
          .csv(os.path.join(SAMPLE, fname)))
    return medicare.preprocess(df)


def _by_name(report, name):
    return next(r for r in report.results if r.check_name == name)


def test_beneficiary_profile_catches_injected_issues(spark):
    df = _load(spark, "beneficiary")
    report = DQEngine(spark).run(df, medicare.checks_for("beneficiary"),
                                 table_name="beneficiary")
    assert _by_name(report, "BENE_RACE_CD_accepted_values").status == "fail"
    assert _by_name(report, "BENE_BIRTH_DT_before_BENE_DEATH_DT").status == "fail"
    assert _by_name(report, "DESYNPUF_ID_unique").status == "pass"
    assert _by_name(report, "beneficiary_schema").status == "pass"
    assert report.overall_status == "FAIL"


def test_inpatient_profile_catches_injected_issues(spark):
    bene = _load(spark, "beneficiary")
    df = _load(spark, "inpatient")
    report = DQEngine(spark).run(df, medicare.checks_for("inpatient"),
                                 table_name="inpatient",
                                 ref_tables={"beneficiary": bene})
    assert _by_name(report, "CLM_ID_unique").status == "fail"
    assert _by_name(report, "CLM_FROM_DT_before_CLM_THRU_DT").status == "fail"
    assert _by_name(report, "claim_beneficiary_link").status == "fail"  # orphan claim
    assert _by_name(report, "CLM_PMT_AMT_in_range").status == "fail"    # negative payment
    assert _by_name(report, "inpatient_schema").status == "pass"
    assert report.overall_status == "FAIL"


def test_outpatient_and_carrier_profiles_run(spark):
    bene = _load(spark, "beneficiary")
    for name in ("outpatient", "carrier"):
        df = _load(spark, name)
        report = DQEngine(spark).run(df, medicare.checks_for(name),
                                     table_name=name,
                                     ref_tables={"beneficiary": bene})
        # synthetic demo rows are valid for these two tables
        assert _by_name(report, f"{name}_schema").status == "pass"
        assert report.overall_status in ("PASS", "FAIL")
        assert len(report.results) > 5
        md = report.to_markdown()
        assert f"# Data Quality Report" in md


def test_preprocess_casts_dates_and_amounts(spark):
    df = _load(spark, "inpatient")
    dtypes = dict(df.dtypes)
    assert dtypes["CLM_FROM_DT"] == "date"
    assert dtypes["CLM_PMT_AMT"] == "double"
    assert dtypes["CLM_ID"] == "string"
