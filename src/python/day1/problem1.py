import os
import sys
import requests
from pyspark.sql import SparkSession

# Set the Python path
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["HADOOP_COMMON_LIB_NATIVE_DIR"] = rf'{os.environ["HADOOP_HOME"]}\lib\native'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON Data") \
    .getOrCreate()

# Download JSON data from the URL
url = "https://ipinfo.io/161.185.160.93/geo"
response = requests.get(url)
json_data = response.json()

# Read JSON data into a DataFrame
data = spark.read.json(spark.sparkContext.parallelize([json_data]))

# Perform operations on the data
# For example, you can display the schema and show the data
data.printSchema()
data.show()

# Stop the SparkSession
spark.stop()
