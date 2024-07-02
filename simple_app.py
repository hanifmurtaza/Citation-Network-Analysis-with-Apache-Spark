import os
from pyspark.sql import SparkSession

# Set Hadoop DLL path
os.environ['HADOOP_HOME'] = 'C:/hadoop-3.4.0'
os.environ['PATH'] += os.pathsep + 'C:/hadoop-3.4.0/bin'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Citation Network Analysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Load the JSON dataset
data_path = "C:/Users/HANIF/Downloads/dblp_v14.json/dblp-citation-network-v14.json"  # Adjust the path accordingly
df = spark.read.json(data_path)

# Show the schema to verify
df.printSchema()

# Directory path to save the Parquet file
save_path = "C:/Users/HANIF/Downloads/dblp_v14.json/dblp_v14.parquet"

# Save the DataFrame to a Parquet file with overwrite mode
df.write.mode("overwrite").parquet(save_path)

print("Data loaded and saved as Parquet format.")
spark.stop()
