from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

@dp.materialized_view(
    name=f"{catalog}.gold.gold_transactions_summary",
    comment="Aggregated transaction summary by branch and transaction type"
)
def gold_transactions_summary():
    return (
        spark.read.table(f"{catalog}.silver.silver_transactions")
        .groupBy("branch", "transaction_type")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("total_amount_usd"),
            F.avg("amount_usd").alias("avg_amount_usd"),
            F.min("amount_usd").alias("min_amount_usd"),
            F.max("amount_usd").alias("max_amount_usd")
        )
    )
