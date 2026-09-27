"""Generate small SYNTHETIC demo CSVs shaped like the CMS DE-SynPUF files.

These files exist so the example runs end-to-end without downloading the
real CMS dataset. They intentionally contain a few data-quality problems
(a duplicate claim id, an out-of-range race code, an inverted date range,
an orphan claim) so the demo report shows real findings.

Do NOT treat this as real Medicare data.
"""

import csv
import datetime as dt
import os
import random

random.seed(42)
OUT = os.path.join(os.path.dirname(__file__), "sample")


def _rand_date(start: dt.date, end: dt.date) -> dt.date:
    """A uniformly random date in [start, end] using real date arithmetic."""
    return start + dt.timedelta(days=random.randint(0, (end - start).days))


def _ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def write(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def beneficiary_header():
    chronic = ["SP_ALZHDMTA", "SP_CHF", "SP_CHRNKIDN", "SP_CNCR", "SP_COPD",
               "SP_DEPRESSN", "SP_DIABETES", "SP_ISCHMCHT", "SP_OSTEOPRS",
               "SP_RA_OA", "SP_STRKETIA"]
    return (["DESYNPUF_ID", "BENE_BIRTH_DT", "BENE_DEATH_DT", "BENE_SEX_IDENT_CD",
             "BENE_RACE_CD", "BENE_ESRD_IND", "SP_STATE_CODE", "BENE_COUNTY_CD"]
            + chronic
            + ["BENE_HI_CVRAGE_TOT_MONS", "BENE_SMI_CVRAGE_TOT_MONS",
               "BENE_HMO_CVRAGE_TOT_MONS", "PLAN_CVRG_MOS_NUM",
               "MEDREIMB_IP", "BENRES_IP", "PPPYMT_IP",
               "MEDREIMB_OP", "BENRES_OP", "PPPYMT_OP",
               "MEDREIMB_CAR", "BENRES_CAR", "PPPYMT_CAR"])


def beneficiary_rows(n=12):
    rows = []
    for i in range(n):
        bid = f"BENE{i:05d}"
        birth = _ymd(_rand_date(dt.date(1920, 1, 1), dt.date(1950, 12, 31)))
        death = ("" if random.random() < 0.7
                 else _ymd(_rand_date(dt.date(2008, 1, 1), dt.date(2010, 12, 31))))
        race = random.choice(["1", "2", "3", "4", "5"])
        rows.append(
            [bid, birth, death, random.choice(["1", "2"]), race,
             random.choice(["0", "Y"]), "CA", "06001"]
            + [random.choice(["Y", "N"]) for _ in range(11)]
            + [12, 12, 0, 0,
               round(random.uniform(0, 20000), 2), round(random.uniform(0, 3000), 2),
               round(random.uniform(0, 15000), 2),
               round(random.uniform(0, 8000), 2), round(random.uniform(0, 1500), 2),
               round(random.uniform(0, 6000), 2),
               round(random.uniform(0, 9000), 2), round(random.uniform(0, 1200), 2),
               round(random.uniform(0, 7000), 2)])
    # injected issues:
    rows[3][4] = "9"      # invalid race code
    rows[5][1], rows[5][2] = "19900101", "19850101"  # birth after death
    return rows


def inpatient_header():
    return (["DESYNPUF_ID", "CLM_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT",
             "PRVDR_NUM", "CLM_PMT_AMT", "NCH_PRMRY_PYR_CLM_PD_AMT",
             "AT_PHYSN_NPI", "OP_PHYSN_NPI", "OT_PHYSN_NPI",
             "CLM_ADMSN_DT", "ADMTNG_ICD9_DGNS_CD",
             "CLM_PASS_THRU_PER_DIEM_AMT", "NCH_BENE_IP_DDCTBL_AMT",
             "NCH_BENE_PTA_COINSRNC_LBLTY_AM", "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM",
             "CLM_UTLZTN_DAY_CNT", "NCH_BENE_DSCHRG_DT", "CLM_DRG_CD"]
            + [f"ICD9_DGNS_CD_{i}" for i in range(1, 11)]
            + [f"ICD9_PRCDR_CD_{i}" for i in range(1, 7)]
            + [f"HCPCS_CD_{i}" for i in range(1, 46)])


def inpatient_rows(n=10):
    rows = []
    for i in range(n):
        frm = _rand_date(dt.date(2008, 1, 1), dt.date(2010, 11, 1))
        thru = frm + dt.timedelta(days=random.randint(1, 20))
        frm_s, thru_s = _ymd(frm), _ymd(thru)
        rows.append(
            [f"BENE{i:05d}", f"CLM{i:06d}", random.choice(["1", "2"]),
             frm_s, thru_s, f"{random.randint(10000, 99999)}",
             round(random.uniform(500, 60000), 2), 0.0,
             f"{random.randint(10**9, 10**10 - 1)}", "", "",
             frm_s, "41071", 0.0, 1068.0, 200.0, 0.0,
             random.randint(1, 14), thru_s, "291",
             *["41071"] + [""] * 9, *["3615"] + [""] * 5, *[""] * 45])
    # injected issues:
    rows[2][1] = rows[0][1]          # duplicate CLM_ID
    rows[4][3], rows[4][4] = "20100601", "20100501"  # from > thru
    rows[6][0] = "GHOST99999"        # orphan beneficiary id
    rows[7][6] = -250.0              # negative payment
    return rows


def outpatient_rows(n=8):
    base = [c for c in inpatient_header() if c != "CLM_UTLZTN_DAY_CNT"]
    header = base + ["NCH_BENE_PTB_DDCTBL_AMT", "NCH_BENE_PTB_COINSRNC_AMT"]
    rows = []
    for i in range(n):
        frm = _rand_date(dt.date(2008, 1, 1), dt.date(2010, 11, 1))
        thru = frm + dt.timedelta(days=random.randint(0, 5))
        frm_s, thru_s = _ymd(frm), _ymd(thru)
        row = dict(zip(inpatient_header(), [
            f"BENE{i:05d}", f"OCLM{i:06d}", "1", frm_s, thru_s,
            f"{random.randint(10000, 99999)}",
            round(random.uniform(50, 4000), 2), 0.0,
            f"{random.randint(10**9, 10**10 - 1)}", "", "",
            "", "78650", 0.0, 0.0, 0.0, 0.0,
            random.randint(1, 5), "", "",
            *["78650"] + [""] * 9, *[""] * 6, *["99213"] + [""] * 44]))
        rows.append([row[c] for c in base]
                    + [round(random.uniform(0, 200), 2),
                       round(random.uniform(0, 100), 2)])
    return header, rows


def carrier_rows(n=8):
    header = (["DESYNPUF_ID", "CLM_ID", "CLM_FROM_DT", "CLM_THRU_DT"]
              + [f"ICD9_DGNS_CD_{i}" for i in range(1, 9)]
              + [f"PRF_PHYSN_NPI_{i}" for i in range(1, 14)]
              + [f"TAX_NUM_{i}" for i in range(1, 14)]
              + [f"HCPCS_CD_{i}" for i in range(1, 14)]
              + [f"LINE_NCH_PMT_AMT_{i}" for i in range(1, 14)]
              + [f"LINE_BENE_PTB_DDCTBL_AMT_{i}" for i in range(1, 14)]
              + [f"LINE_BENE_PRMRY_PYR_PD_AMT_{i}" for i in range(1, 14)]
              + [f"LINE_COINSRNC_AMT_{i}" for i in range(1, 14)]
              + [f"LINE_ALOWD_CHRG_AMT_{i}" for i in range(1, 14)]
              + [f"LINE_PRCSG_IND_CD_{i}" for i in range(1, 14)]
              + [f"LINE_ICD9_DGNS_CD_{i}" for i in range(1, 14)])
    rows = []
    for i in range(n):
        frm = _rand_date(dt.date(2008, 1, 1), dt.date(2010, 11, 1))
        npi = f"{random.randint(10**9, 10**10 - 1)}"
        frm_s, thru_s = _ymd(frm), _ymd(frm + dt.timedelta(days=1))
        rows.append(
            [f"BENE{i:05d}", f"CCLM{i:06d}", frm_s, thru_s,
             "4019"] + [""] * 7
            + [npi] + [""] * 12 + ["123456789"] + [""] * 12
            + ["99213"] + [""] * 12
            + [round(random.uniform(20, 300), 2)] + [0.0] * 12
            + [0.0] * 13 + [0.0] * 13
            + [round(random.uniform(5, 60), 2)] + [0.0] * 12
            + [round(random.uniform(25, 350), 2)] + [0.0] * 12
            + ["A"] + [""] * 12 + ["4019"] + [""] * 12)
    return header, rows


def main():
    write(f"{OUT}/beneficiary.csv", beneficiary_header(), beneficiary_rows())
    write(f"{OUT}/inpatient.csv", inpatient_header(), inpatient_rows())
    h, r = outpatient_rows()
    write(f"{OUT}/outpatient.csv", h, r)
    h, r = carrier_rows()
    write(f"{OUT}/carrier_b.csv", h, r)
    print(f"Synthetic demo CSVs written to {OUT}/")


if __name__ == "__main__":
    main()
