# Databricks notebook source
# MAGIC %md
# MAGIC bronze layer
# MAGIC

# COMMAND ----------

df=spark.read.csv("/Volumes/retail_project/raw_data/raw_file/retail_customers_messy.csv",
                  sep="," ,header=True,inferSchema=True)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df2=spark.read.csv("/Volumes/retail_project/raw_data/raw_file/retail_orders_messy.csv",
                  sep="," ,header=True,inferSchema=True)
display(df2)


# COMMAND ----------

df2.count()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("store_location", StringType(), True),
    StructField("order_status", StringType(), True)
])

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("loyalty_points", StringType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

orders_df = spark.read \
    .format("csv") \
    .option("header","true") \
    .schema(orders_schema) \
    .load("/Volumes/retail_project/raw_data/raw_file/retail_orders_messy.csv")


customers_df=spark.read.format("csv") \
    .option("header", "true") \
    .schema(customer_schema) \
    .load("/Volumes/retail_project/raw_data/raw_file/retail_customers_messy.csv")


# COMMAND ----------

orders_df.display()
customers_df.display()

# COMMAND ----------

orders_df.count()


# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
orders_df=orders_df.withColumn('ingest_timestamp',current_timestamp())
customers_df=customers_df.withColumn('ingest_timestamp',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC save as delta table
# MAGIC

# COMMAND ----------

orders_df.write.format("delta").mode("overwrite").saveAsTable('retail_project.bronze.bronze_orders')
customers_df.write.format("delta").mode("overwrite").saveAsTable('retail_project.bronze.bronze_customers')

# COMMAND ----------

bronze_customers_df=spark.read.table('retail_project.bronze.bronze_customers')
display(bronze_customers_df)

# COMMAND ----------

bronze_orders_df=spark.read.table('retail_project.bronze.bronze_orders')
display(bronze_orders_df)


# COMMAND ----------

from pyspark.sql.functions import try_to_date, col, coalesce

silver_df = bronze_orders_df.withColumn(
     'order_date',
     coalesce(
         try_to_date(col('order_date'), 'yyyy/MM/dd'),
         try_to_date(col('order_date'), 'yyyy-MM-dd'),
         try_to_date(col('order_date'), 'dd-MM-yyyy'),
         try_to_date(col('order_date'), 'dd/MM/yyyy')
        )
)
display(silver_df)

# COMMAND ----------

silver_df.select('category').distinct().display()

# COMMAND ----------

from pyspark.sql.functions import col
df1=silver_df.select(col('store_location'),
                     col(  'order_status'))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import upper,trim,when

silver_df=silver_df.withColumn('category',
                               upper(trim(col('category')))
)

silver_df=silver_df.withColumn(
    'category',
    when(col('category')=='ELECTRONIC','ELECTRONICS').
    when(col('category')=='HOME AND LIVING','HOME & LIVING').
    when(col('category')=='FASHION','CLOTHING').
    otherwise(col('category')) 
)



# COMMAND ----------

from pyspark.sql.functions import count,countDistinct

silver_df.select(count('order_id'),countDistinct('order_id')).display()

# COMMAND ----------

silver_df = silver_df.dropDuplicates(['order_id'])


silver_df.select(count('order_id'),countDistinct('order_id')).display()

# COMMAND ----------

from pyspark.sql.functions import when

silver_df=silver_df.withColumn(
    'discount',
    when(col('discount').isNull(),'0').when(col('discount')=='ten','0.1').otherwise(col('discount'))
)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

from pyspark.sql.functions import when

silver_df=silver_df.withColumn(
    'discount',
    when(col('discount')=='5%','0').when(col('discount')=='10%','0.1').otherwise(col('discount'))
)

silver_df.display()

# COMMAND ----------

silver_df=silver_df.filter(col('quantity')>=0)

silver_df.display()

# COMMAND ----------

from pyspark.sql.types import DecimalType

silver_df=silver_df.withColumn(
    'unit_price',
    col('unit_price').try_cast(DecimalType(10,2))

)

display(silver_df)

# COMMAND ----------

silver_df=silver_df.withColumn(
    'total_amount',
    col('quantity').cast("int") * col("unit_price") * (1 - col("discount").cast("double"))
)

silver_df.display()

# COMMAND ----------

silver_df.write.format('delta').mode('overwrite').saveAsTable('retail_project.silver.silver_orders')

# COMMAND ----------

customers_df.display()

# COMMAND ----------

from pyspark.sql.functions import trim, col

customers_df = bronze_customers_df.select(
    trim(col("customer_id")).alias("customer_id"),
    trim(col("customer_name")).alias("customer_name"),
    trim(col("email")).alias("email"),
    trim(col("phone")).alias("phone"),
    trim(col("gender")).alias("gender"),
    trim(col("date_of_birth")).alias("date_of_birth"),
    trim(col("city")).alias("city"),
    trim(col("state")).alias("state"),
    trim(col("registration_date")).alias("registration_date"),
    trim(col("loyalty_points")).alias("loyalty_points"),
    trim(col("status")).alias("status"),
    col("ingest_timestamp")
)

display(customers_df)

# COMMAND ----------

