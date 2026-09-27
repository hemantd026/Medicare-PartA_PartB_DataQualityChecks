import os
import sys

import pytest
from pyspark.sql import SparkSession

# Make Spark launch workers with this exact interpreter (it carries pyspark).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("sparkdq-tests")
         .master("local[2]")
         # Skip the forking Python daemon: in locked-down containers its stdin
         # can be at EOF, which kills it instantly ("Python daemon failed to
         # launch worker"). Plain workers are slower per task but run anywhere.
         .config("spark.python.use.daemon", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
