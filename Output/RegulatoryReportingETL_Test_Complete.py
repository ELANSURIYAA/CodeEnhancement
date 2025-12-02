# ====================================================================
# Author: Ascendion AAVA
# Date: <Leave it blank>
# Description: Complete Python-based test script for RegulatoryReportingETL Pipeline (without PyTest framework)
# ====================================================================

import sys
import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType