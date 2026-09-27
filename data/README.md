# Getting the real CMS Medicare Part A/B data

The checker ships with synthetic demo CSVs (`examples/sample/`) so it runs
out of the box. To validate **real** data, download the CMS public files:

1. Go to the CMS page: **Medicare Claims Synthetic Public Use Files (SynPUFs)**
   https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files
2. Download **DE 1.0, Sample 1 (2008–2010)** — public domain, no data-use
   agreement required. You want:
   - `DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv` (also 2009, 2010)
   - `DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv`
   - `DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv`
   - `DE1_0_2008_to_2010_Carrier_Claims_Sample_1.csv` (contains Part A & B)
3. Rename/copy them into one folder as:
   - `beneficiary.csv`, `inpatient.csv`, `outpatient.csv`, `carrier_b.csv`
4. Run: `python examples/run_medicare_checks.py --data /path/to/that/folder`

Notes:
- Raw dates are `YYYYMMDD` integers; `medicare.preprocess()` casts them.
- The carrier file holds both Part A and Part B claim lines (13 per row);
  the profile treats them uniformly.
- Link claims to beneficiaries on `DESYNPUF_ID`. Use `CLM_THRU_DT` to
  attribute a claim to a calendar year.
