from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

@dp.table(
    name=f"{catalog}.bronze.bronze_transactions",
    comment="Raw financial transactions ingested from CSV files via Auto Loader"
)
def bronze_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{catalog}/bronze/raw/input/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
    )
