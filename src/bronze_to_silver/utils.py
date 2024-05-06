# Databricks notebook source
# Mount Bronze Blob Storage using dbutils
storage_account_name = "adfstoragesriram"
container_name = "bronze"
mount_point = "/mnt/Bronze"
AccessKey = dbutils.secrets.get('BlobBronzeAccessKey', 'BronzeAccessKey')

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=mount_point,
  extra_configs={
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey
  }
)

# COMMAND ----------

# Mount Silver Blob Storage using dbutils
storage_account_name = "adfstoragesriram"
container_name = "silver"
mount_point = "/mnt/Silver"
AccessKey = dbutils.secrets.get('BlobBronzeAccessKey', 'BronzeAccessKey')

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=mount_point,
  extra_configs={
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey
  }
)

# COMMAND ----------

# Mount Gold Blob Storage using dbutils
storage_account_name = "adfstoragesriram"
container_name = "gold"
mount_point = "/mnt/gold"
AccessKey = dbutils.secrets.get('BlobBronzeAccessKey', 'BronzeAccessKey')

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=mount_point,
  extra_configs={
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey
  }
)

# COMMAND ----------

from pyspark.sql.functions import udf
def toSnakeCase(df):
    for column in df.columns:
        snake_case_col = ''
        for char in column:
            if char ==' ':
                snake_case_col += '_'
            else:
                snake_case_col += char.lower()
        df = df.withColumnRenamed(column, snake_case_col)
    return df

udf(toSnakeCase)

# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)
