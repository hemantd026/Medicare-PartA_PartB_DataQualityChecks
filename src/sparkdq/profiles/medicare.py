"""Data-quality profiles for the CMS Medicare Part A/B public dataset.

Dataset: CMS 2008-2010 Data Entrepreneurs' Synthetic Public Use Files
(DE-SynPUF), Sample 1. Public domain, no data-use agreement required.
Download: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files

File layout (Sample 1):
  * Beneficiary Summary (per year: 2008/2009/2010) -- demographics, coverage
    months, chronic-condition flags, annual reimbursement summaries.
  * Inpatient Claims (2008-2010, "Part A"-type institutional claims)
  * Outpatient Claims (2008-2010, "Part B"-type institutional claims)
  * Carrier Claims Part A / Part B (2008-2010, physician/supplier claims,
    13 line-items per row)

Part A  = inpatient hospital stays, skilled nursing, hospice.
Part B  = doctors' services, outpatient care, preventive services, DME.
Link key across every file: DESYNPUF_ID (synthetic beneficiary id).

Column names follow the official CMS DE-SynPUF codebook. Raw CSV dates are
YYYYMMDD integers; :func:`preprocess` casts them to proper ``date`` columns
and monetary/count columns to numeric types.
"""

from __future__ import annotations

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType

from ..checks import (
    Check, accepted_values, custom_sql, date_order, not_null, numeric_range,
    referential_integrity, row_count, schema_match, unique,
)

# ---------------------------------------------------------------------------
# Value domains (CMS DE-SynPUF codebook)
# ---------------------------------------------------------------------------
SEX_CODES = ["1", "2"]                       # 1=Male, 2=Female
RACE_CODES = ["1", "2", "3", "4", "5"]       # White/Black/Others/Asian/Hispanic
ESRD_IND = ["0", "Y"]                        # 0=No ESRD, Y=ESRD
CHRONIC_FLAGS = ["Y", "N"]                   # chronic-condition indicators
SEGMENTS = ["1", "2"]                        # claim line segment

CHRONIC_COLS = ["SP_ALZHDMTA", "SP_CHF", "SP_CHRNKIDN", "SP_CNCR", "SP_COPD",
                "SP_DEPRESSN", "SP_DIABETES", "SP_ISCHMCHT", "SP_OSTEOPRS",
                "SP_RA_OA", "SP_STRKETIA"]

# ---------------------------------------------------------------------------
# Column layouts (post-preprocess types)
# ---------------------------------------------------------------------------

def _str_cols(*names: str) -> Dict[str, str]:
    return {n: "string" for n in names}


def _dgcols(prefix: str, n: int) -> List[str]:
    return [f"{prefix}_{i}" for i in range(1, n + 1)]


BENEFICIARY_COLUMNS = (
    ["DESYNPUF_ID", "BENE_BIRTH_DT", "BENE_DEATH_DT", "BENE_SEX_IDENT_CD",
     "BENE_RACE_CD", "BENE_ESRD_IND", "SP_STATE_CODE", "BENE_COUNTY_CD"]
    + CHRONIC_COLS
)

_BENE_MONTHS = ["BENE_HI_CVRAGE_TOT_MONS", "BENE_SMI_CVRAGE_TOT_MONS",
                "BENE_HMO_CVRAGE_TOT_MONS", "PLAN_CVRG_MOS_NUM"]
_BENE_AMOUNTS = ["MEDREIMB_IP", "BENRES_IP", "PPPYMT_IP",
                 "MEDREIMB_OP", "BENRES_OP", "PPPYMT_OP",
                 "MEDREIMB_CAR", "BENRES_CAR", "PPPYMT_CAR"]

BENEFICIARY_SCHEMA: Dict[str, str] = {
    **_str_cols(*BENEFICIARY_COLUMNS),
    **{c: "int" for c in _BENE_MONTHS},
    **{c: "double" for c in _BENE_AMOUNTS},
    "BENE_BIRTH_DT": "date", "BENE_DEATH_DT": "date",
}

_CLAIM_IDS = ["DESYNPUF_ID", "CLM_ID", "SEGMENT", "PRVDR_NUM",
              "AT_PHYSN_NPI", "OP_PHYSN_NPI", "OT_PHYSN_NPI",
              "ADMTNG_ICD9_DGNS_CD", "CLM_DRG_CD"]
_CLAIM_DATES = ["CLM_FROM_DT", "CLM_THRU_DT", "CLM_ADMSN_DT", "NCH_BENE_DSCHRG_DT"]
# amounts present on every institutional claim file
_BASE_AMOUNTS = ["CLM_PMT_AMT", "NCH_PRMRY_PYR_CLM_PD_AMT",
                 "CLM_PASS_THRU_PER_DIEM_AMT", "NCH_BENE_IP_DDCTBL_AMT",
                 "NCH_BENE_PTA_COINSRNC_LBLTY_AM",
                 "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM"]
# Part B deductible/coinsurance only exist on the outpatient file
_PTB_AMOUNTS = ["NCH_BENE_PTB_DDCTBL_AMT", "NCH_BENE_PTB_COINSRNC_AMT"]

INPATIENT_SCHEMA: Dict[str, str] = {
    **_str_cols(*_CLAIM_IDS, *_dgcols("ICD9_DGNS_CD", 10),
                *_dgcols("ICD9_PRCDR_CD", 6), *_dgcols("HCPCS_CD", 45)),
    **{c: "date" for c in _CLAIM_DATES},
    **{c: "double" for c in _BASE_AMOUNTS},
    "CLM_UTLZTN_DAY_CNT": "int",
}

OUTPATIENT_SCHEMA: Dict[str, str] = {
    **_str_cols(*_CLAIM_IDS, *_dgcols("ICD9_DGNS_CD", 10),
                *_dgcols("ICD9_PRCDR_CD", 6), *_dgcols("HCPCS_CD", 45)),
    **{c: "date" for c in _CLAIM_DATES},
    **{c: "double" for c in _BASE_AMOUNTS + _PTB_AMOUNTS},
}

_CARRIER_LINE_AMOUNTS = ["LINE_NCH_PMT_AMT", "LINE_BENE_PTB_DDCTBL_AMT",
                         "LINE_BENE_PRMRY_PYR_PD_AMT", "LINE_COINSRNC_AMT",
                         "LINE_ALOWD_CHRG_AMT"]
_CARRIER_STR = (["DESYNPUF_ID", "CLM_ID"]
                + _dgcols("ICD9_DGNS_CD", 8)
                + _dgcols("PRF_PHYSN_NPI", 13) + _dgcols("TAX_NUM", 13)
                + _dgcols("HCPCS_CD", 13) + _dgcols("LINE_PRCSG_IND_CD", 13)
                + _dgcols("LINE_ICD9_DGNS_CD", 13))

CARRIER_SCHEMA: Dict[str, str] = {
    **_str_cols(*_CARRIER_STR),
    "CLM_FROM_DT": "date", "CLM_THRU_DT": "date",
    **{f"{p}_{i}": "double" for p in _CARRIER_LINE_AMOUNTS for i in range(1, 14)},
}

SCHEMAS: Dict[str, Dict[str, str]] = {
    "beneficiary": BENEFICIARY_SCHEMA,
    "inpatient": INPATIENT_SCHEMA,
    "outpatient": OUTPATIENT_SCHEMA,
    "carrier": CARRIER_SCHEMA,   # shared by carrier Part A and Part B files
}

# ---------------------------------------------------------------------------
# Preprocessing: raw CSVs -> typed DataFrames
# ---------------------------------------------------------------------------

def preprocess(df: DataFrame) -> DataFrame:
    """Cast raw DE-SynPUF string columns to proper types.

    * ``*_DT``            -> ``date``  (from YYYYMMDD)
    * ``*_AMT``           -> ``double``
    * ``*_MONS``, ``*_CNT``, ``PLAN_CVRG_MOS_NUM`` -> ``int``
    Everything else stays ``string``.
    """
    out = df
    for c in df.columns:
        if c.endswith("_DT"):
            # try_to_date: malformed dates become NULL (caught by not_null /
            # date checks) instead of aborting the whole job in ANSI mode.
            out = out.withColumn(c, F.try_to_date(F.col(c).cast("string"), "yyyyMMdd"))
        elif (c.endswith(("_AMT", "_AM")) or "_AMT_" in c
                or c.startswith(("MEDREIMB_", "BENRES_", "PPPYMT_"))):
            # _AMT / _AM suffixes, LINE_*_AMT_* line items, and the beneficiary
            # MEDREIMB_*/BENRES_*/PPPYMT_* reimbursement columns.
            out = out.withColumn(c, F.col(c).cast("double"))
        elif c.endswith(("_MONS", "_CNT")) or c == "PLAN_CVRG_MOS_NUM":
            out = out.withColumn(c, F.col(c).cast("int"))
    return out


# ---------------------------------------------------------------------------
# Check suites
# ---------------------------------------------------------------------------

def _base_claim_checks() -> List[Check]:
    return [
        row_count(1),
        not_null("DESYNPUF_ID"),
        not_null("CLM_ID"),
        unique("CLM_ID"),
        date_order("CLM_FROM_DT", "CLM_THRU_DT"),
        numeric_range("CLM_PMT_AMT", min_value=0, severity="warning",
                      description="Claim payment amounts are expected to be non-negative"),
        Check(name="claim_beneficiary_link", check_type="referential_integrity",
              column="DESYNPUF_ID",
              params={"parent_column": "DESYNPUF_ID", "ref_table": "beneficiary"},
              severity="error",
              description="Every claim must link to a beneficiary via DESYNPUF_ID"),
    ]


def beneficiary_checks() -> List[Check]:
    checks: List[Check] = [
        row_count(1),
        schema_match(BENEFICIARY_SCHEMA, name="beneficiary_schema"),
        not_null("DESYNPUF_ID"),
        unique("DESYNPUF_ID"),
        accepted_values("BENE_SEX_IDENT_CD", SEX_CODES),
        accepted_values("BENE_RACE_CD", RACE_CODES),
        accepted_values("BENE_ESRD_IND", ESRD_IND),
        date_order("BENE_BIRTH_DT", "BENE_DEATH_DT",
                   description="Birth date must be on or before death date"),
        custom_sql("coverage_months_sane",
                   "BENE_HI_CVRAGE_TOT_MONS BETWEEN 0 AND 12 "
                   "AND BENE_SMI_CVRAGE_TOT_MONS BETWEEN 0 AND 12",
                   "HI/SMI coverage months must be between 0 and 12"),
    ]
    for col in CHRONIC_COLS:
        checks.append(accepted_values(col, CHRONIC_FLAGS, severity="warning",
                                      description=f"Chronic flag {col} expected to be Y/N"))
    for col in _BENE_AMOUNTS:
        checks.append(numeric_range(col, min_value=0, severity="warning",
                                    description=f"{col} expected to be non-negative"))
    return checks


def inpatient_checks() -> List[Check]:
    return [
        schema_match(INPATIENT_SCHEMA, name="inpatient_schema"),
        *_base_claim_checks(),
        date_order("CLM_ADMSN_DT", "NCH_BENE_DSCHRG_DT",
                   description="Admission date must be on or before discharge date"),
        numeric_range("CLM_UTLZTN_DAY_CNT", min_value=0,
                      description="Utilization day count must be non-negative"),
        accepted_values("SEGMENT", SEGMENTS, severity="warning"),
        numeric_range("NCH_BENE_IP_DDCTBL_AMT", min_value=0, severity="warning"),
    ]


def outpatient_checks() -> List[Check]:
    return [
        schema_match(OUTPATIENT_SCHEMA, name="outpatient_schema"),
        *_base_claim_checks(),
        numeric_range("NCH_BENE_PTB_DDCTBL_AMT", min_value=0, severity="warning"),
        numeric_range("NCH_BENE_PTB_COINSRNC_AMT", min_value=0, severity="warning"),
    ]


def carrier_checks() -> List[Check]:
    checks: List[Check] = [
        schema_match(CARRIER_SCHEMA, name="carrier_schema"),
        row_count(1),
        not_null("DESYNPUF_ID"),
        not_null("CLM_ID"),
        unique("CLM_ID"),
        date_order("CLM_FROM_DT", "CLM_THRU_DT"),
        Check(name="claim_beneficiary_link", check_type="referential_integrity",
              column="DESYNPUF_ID",
              params={"parent_column": "DESYNPUF_ID", "ref_table": "beneficiary"},
              severity="error",
              description="Every claim must link to a beneficiary via DESYNPUF_ID"),
    ]
    for p in _CARRIER_LINE_AMOUNTS:
        for i in range(1, 14):
            checks.append(numeric_range(f"{p}_{i}", min_value=0, severity="warning",
                                        description=f"{p}_{i} expected to be non-negative"))
    return checks


def checks_for(table: str) -> List[Check]:
    """Return the check suite for a DE-SynPUF table.

    ``table`` is one of: beneficiary, inpatient, outpatient, carrier.
    """
    suites = {"beneficiary": beneficiary_checks,
              "inpatient": inpatient_checks,
              "outpatient": outpatient_checks,
              "carrier": carrier_checks}
    if table not in suites:
        raise ValueError(f"Unknown table {table!r}; choose from {sorted(suites)}")
    return suites[table]()
