# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("Customer_ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Age", StringType(), True),  # Age stored as STRING instead of INTEGER
    StructField("Purchase_Amount", StringType(), True),  # Should be DOUBLE
    StructField("Signup_Date", StringType(), True)  # Dates are in different formats
])


# COMMAND ----------

data = [
    (1, "Alice", "alice@example.com", "25", "100.0", "2024-01-10"),
    (2, "Bob", "bob@example.com", None, "500.5", "01-12-2024"),  # Missing Age, Wrong Date Format
    (3, "Charlie", "charlie@domain", "30", None, "2024/02/15"),  # Invalid email, Missing Amount
    (4, "David", None, "28", "300.0", "05/03/2024"),  # Missing Email, Inconsistent Date
    (5, "Eve", "eve@example.com", "Twenty-Two", "700.75", "2024-01-25"),  # Age as words
    (1, "Alice", "alice@example.com", "25", "100.0", "2024-01-10"),  # Duplicate Record
    (7, "John", "  JOHN@GMAIL.COM  ", "40", "50.5", "Feb 20, 2024"),  # Extra spaces, inconsistent email case
    (8, "Mike", "mike@example.com", "35", "NA", "2024-01-30"),  # 'NA' as invalid value
]


# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()


# COMMAND ----------

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).display()


# COMMAND ----------

df_fixed = df.withColumn("Age", regexp_replace(col("Age"), "[^0-9]", "").cast(IntegerType())) \
             .withColumn("Purchase_Amount", when(col("Purchase_Amount") == "NA", None).otherwise(col("Purchase_Amount")).cast(DoubleType())) \
             .withColumn("Signup_Date", to_date(regexp_replace(col("Signup_Date"), "/", "-"), "yyyy-MM-dd"))\
                 .withColumn("Email", trim((col("Email"))))


# COMMAND ----------

df_fixed.display()

# COMMAND ----------

df_fixed.printSchema()


# COMMAND ----------

final_df = df_fixed.distinct()
final_df.display()
