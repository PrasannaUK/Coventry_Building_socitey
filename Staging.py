#Read from raw layer, cleanse & validate
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date

def ingest_data(path: str) -> DataFrame:
  df=spark\
  .read\
  .option('header','true')\
  .option('inferSchema','true')\
  .csv(path)      #reading csv file
  print(f"Total records ingested to staging: {df.count()}")
  return df

#Transformation – Cleaning & Validation
def clean_data(df:DataFrame) -> DataFrame:
  cleaned_df = df.dropna(subset=["transaction_id", "account_number", "amount"])\
                  .withColumn('transaction_date',col('transaction_date').cast('date'))\
                  .filter(F.col('amount') !=0) # Null handling, Invalid record filtering & date conversion
  return cleaned_df

#Injestion into staging Layer
def load_data(df:DataFrame, path:str):
  print(f"Writing clean data to {path}")
  
  df.write.option('header','true').option('inferSchema','true').mode("overwrite").csv(path)

