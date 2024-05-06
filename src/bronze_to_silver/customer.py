
# Databricks notebook source
# MAGIC %run ./Utils

# COMMAND ----------

from pyspark.sql.functions import split, when, col, to_date

# COMMAND ----------

raw_customer_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/customer/20240105_sales_customer.csv', header=True, inferSchema=True)


# COMMAND ----------

renamed_customer_df = toSnakeCase(raw_customer_df)

# COMMAND ----------

splited_name_df = renamed_customer_df.withColumn('first_name', split(renamed_customer_df.name, " ")[0])\
    .withColumn('last_name', split(renamed_customer_df.name, " ")[1]).drop(renamed_customer_df.name)

# COMMAND ----------

extract_domain_df = splited_name_df.withColumn("tempdomain", split(splited_name_df.email_id, "@")[1]).drop(splited_name_df.email_id)
extract_domain_df = extract_domain_df.withColumn('domain', split(extract_domain_df.tempdomain, '\.')[0]).drop(extract_domain_df.tempdomain)

# COMMAND ----------

converted_gender_df = extract_domain_df.withColumn('gender', when(col('gender') == 'male', 'M')\
    .otherwise('F'))

# COMMAND ----------

splited_join_date_df = converted_gender_df.withColumn('date', split(col('joining_date'), " ")[0])\
    .withColumn('time', split(col('joining_date'), ' ')[1]).drop('joining_date')

# COMMAND ----------

converted_date_df = splited_join_date_df.withColumn('date', to_date(col('date'), 'dd-MM-yyyy'))

# COMMAND ----------

expenditure_df = converted_date_df.withColumn('expenditure-status', when(col('spent') < 200, 'MINIMUM').otherwise('MAXIMUM'))

# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/customer'
expenditure_df.write.format('delta').save(writeTo)
