#Pipeline Orchestration
from pyspark.sql.functions import sum as _sum, count, avg, when, col
#from Staging import ingest_data, clean_data, load_data #need to package this module
#from transformed import load_data, enrich_data, aggregate_data, load_data #need to package this module
#import logging #need to package this module

raw_path="s3://raw_bucket/transactions.csv"
staging_path="s3://stage_bucket/clean.csv"
curated_path="s3://core_bucket/content/transformed.parquet"

def main():
  spark= SparkSession \
       .builder \
       .appName("credit_risk") \
       .getOrCreate()
  
    #logging.basicConfig(level=logging.INFO)
    #logger = logging.getLogger("DataOpsPipeline")

    #logger.info("Pipeline started")
    df_inj=ingest_data(raw_path)
    df_cln=clean_data(df_inj)
    load_data(df_cln,staging_path)

    df_cln=stage_data(staging_path)
    df_enrich=enrich_data(df_cln)
    df_aggrt=aggregate_data(df_enrich)
    load_data(df_aggrt,curated_path)


if __name__ == "__main__":
    main()
    print("**** ETL has successfully completed for Credit_risk***")
