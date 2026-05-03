from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

@dp.table(
    name=f"{catalog}.silver.silver_transactions",
    comment="Cleaned and standardized financial transactions"
)
def silver_transactions():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_transactions")
        .filter(F.col("TransactionID").isNotNull())
        .filter(F.col("CustomerID").isNotNull())
        .filter(F.col("AmountUSD").isNotNull())
        .select(
            F.col("TransactionID").alias("transaction_id"),
            F.col("CustomerID").alias("customer_id"),
            F.col("AccountType").alias("account_type"),
            F.col("TransactionType").alias("transaction_type"),
            F.col("AmountUSD").cast("double").alias("amount_usd"),
            F.col("Branch").alias("branch"),
            F.col("TransactionDate").cast("date").alias("transaction_date"),
            F.col("_ingested_at"),
            F.col("_source_file")
        )
    )
