"""Unit tests for every check type in the DQ engine."""

from pyspark.sql import functions as F

from sparkdq import (
    DQEngine, accepted_values, custom_sql, date_order, not_null,
    numeric_range, referential_integrity, regex_match, row_count,
    schema_match, unique,
)
from sparkdq.checks import Check


def make_df(spark):
    return spark.createDataFrame(
        [("a", 1, "2020-01-01", "x@y.com"),
         ("b", 2, "2020-01-05", "bad-email"),
         ("a", 15, "2020-01-03", "z@w.com"),   # dup id, out-of-range n
         (None, 3, "2020-01-10", "q@e.com"),   # null id
         ("c", 4, "2019-12-01", "w@q.com")],   # end before start (row 5)
        ["id", "n", "d", "email"],
    ).withColumn("d", F.to_date("d"))


def run(spark, df, checks, **kw):
    return DQEngine(spark).run(df, checks, table_name="t", **kw)


def status_of(report, name):
    return next(r for r in report.results if r.check_name == name).status


def test_row_count(spark):
    df = make_df(spark)
    assert status_of(run(spark, df, [row_count(1)]), "row_count") == "pass"
    assert status_of(run(spark, df, [row_count(100)]), "row_count") == "fail"


def test_not_null(spark):
    df = make_df(spark)
    r = run(spark, df, [not_null("id")])
    res = next(x for x in r.results if x.check_name == "id_not_null")
    assert res.status == "fail" and res.violating_rows == 1


def test_unique(spark):
    df = make_df(spark)
    r = run(spark, df, [unique("id")])
    res = next(x for x in r.results if x.check_name == "id_unique")
    assert res.status == "fail" and res.violating_rows == 2  # both "a" rows


def test_accepted_values(spark):
    df = make_df(spark)
    r = run(spark, df, [accepted_values("id", ["a", "b", "c"])])
    assert status_of(r, "id_accepted_values") == "pass"
    r = run(spark, df, [accepted_values("id", ["a", "b"])])
    assert status_of(r, "id_accepted_values") == "fail"


def test_numeric_range(spark):
    df = make_df(spark)
    r = run(spark, df, [numeric_range("n", 1, 10)])
    res = next(x for x in r.results if x.check_name == "n_in_range")
    assert res.status == "fail" and res.violating_rows == 1


def test_regex_match(spark):
    df = make_df(spark)
    r = run(spark, df, [regex_match("email", r"^[^@]+@[^@]+\.[^@]+$")])
    res = next(x for x in r.results if x.check_name == "email_regex")
    assert res.status == "fail" and res.violating_rows == 1


def test_date_order(spark):
    df = spark.createDataFrame(
        [("2020-01-01", "2020-01-02"), ("2020-03-01", "2020-02-01")],
        ["s", "e"])
    df = df.withColumn("s", F.to_date("s")).withColumn("e", F.to_date("e"))
    r = run(spark, df, [date_order("s", "e")])
    assert status_of(r, "s_before_e") == "fail"


def test_referential_integrity(spark):
    child = spark.createDataFrame([("a",), ("b",), ("zzz",)], ["cid"])
    parent = spark.createDataFrame([("a",), ("b",)], ["pid"])
    chk = Check(name="ri", check_type="referential_integrity", column="cid",
                params={"parent_column": "pid", "ref_table": "p"})
    r = run(spark, child, [chk], ref_tables={"p": parent})
    res = r.results[0]
    assert res.status == "fail" and res.violating_rows == 1


def test_schema_match(spark):
    df = make_df(spark)
    ok = run(spark, df, [schema_match({"id": "string", "n": "bigint",
                                       "d": "date", "email": "string"})])
    assert status_of(ok, "schema_match") == "pass"
    bad = run(spark, df, [schema_match({"id": "string", "nope": "string"})])
    assert status_of(bad, "schema_match") == "fail"


def test_custom_sql(spark):
    df = make_df(spark)
    r = run(spark, df, [custom_sql("n_positive", "n > 0")])
    assert status_of(r, "n_positive") == "pass"
    r = run(spark, df, [custom_sql("n_small", "n < 10")])
    assert status_of(r, "n_small") == "fail"


def test_overall_status_and_severity(spark):
    df = make_df(spark)
    r = run(spark, df, [not_null("id", severity="warning"),
                        unique("id", severity="error")])
    assert r.overall_status == "FAIL"
    assert r.warnings == 1 and r.failed == 1
    r2 = run(spark, df, [not_null("id", severity="warning")])
    assert r2.overall_status == "PASS" and r2.warnings == 1


def test_broken_check_becomes_error_result(spark):
    df = make_df(spark)
    r = run(spark, df, [not_null("no_such_column")])
    assert r.results[0].status == "error"
    assert r.overall_status == "ERROR"
