====================================================================
Author: Ascendion AAVA
Date: <Leave it blank>
Description: Python-based test script for Enhanced PySpark ETL pipeline (without PyTest framework)
====================================================================

import sys
import os
import tempfile
import shutil
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col