#Read from staging, tranform , aggregate and write into curated layer
from pyspark.sql.functions import sum, count, avg, when, col

#Read from Staging layer
def stage_data(path: str) -> DataFrame:
  df=spark\
  .read\
  .option('header','true')\
  .option('inferSchema','true')\
  .csv(path)      #reading csv file
  print(f"Total records from staging: {df.count()}")
  return df

#Transformation – Enrichment - Business rules, Time-based enrichment, Risk classification
def enrich_data(df:DataFrame):
  df_enrc=df.withColumn(
            "transaction_type",
            F.when(F.col("amount") > 0, "CREDIT").otherwise("DEBIT"))\
            .withColumn("txn_year", F.year("transaction_date"))\
            .withColumn("txn_month", F.month("transaction_date"))\
            .withColumn(
              "risk_flag",
              F.when(F.col("amount") < 1000, "HIGH").otherwise("LOW") #Risk classification
            )
  print("Enriched financial data:")
  return df_enrc

#Transformation – Aggregation 
def aggregate_data(df:DataFrame):
    df_agg = df.groupBy(
            "account_number",
            "transaction_type",
            "txn_year",
            "txn_month"
        )\
        .agg( # Analytical metrics & Reporting
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("transaction_count"),
            F.avg("amount").alias("avg_transaction_amount")
        )
    print("Aggregated Data:")
    return df_agg

#Load Layer – Structured in Parquet
def load_data(df:DataFrame, path:str):
  print(f"Writing tranformed data to {path}")
  
  df.write.option('header','true').option('inferSchema','true').mode("overwrite").parquet(path)
