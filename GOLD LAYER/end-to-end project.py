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

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("customer_id") \
                    .orderBy(desc("ingest_timestamp"))

customers_df = customers_df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

customers_df = customers_df.filter("row_num = 1") \
                           .drop("row_num")

customers_df.display()

# COMMAND ----------

from pyspark.sql.functions import upper, when

customers_df = customers_df.withColumn(
    "status",
    upper(col("status"))
)

customers_df = customers_df.withColumn(
    "status",
    when(col("status").isin("ACTIVE", "INACTIVE"), col("status"))
    .otherwise("UNKNOWN")
)

customers_df.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

customers_df = customers_df.withColumn(
    "loyalty_points",
    regexp_replace(col("loyalty_points"), "[^0-9]", "")
)

customers_df = customers_df.withColumn(
    "loyalty_points",
    col("loyalty_points").try_cast("int")
)

# COMMAND ----------

customers_df.display()

# COMMAND ----------

customers_df = customers_df.withColumn(
    "email_valid",
    col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
)

# Example: meenadas@email.com - valid, karansingh@email - invalid

# customers_df.select("email", "email_valid").display()

customers_df = customers_df.filter(col("email_valid") == True)
    

# COMMAND ----------

from pyspark.sql.functions import to_date, coalesce

customers_df = customers_df.withColumn(
    "dob_parsed",
    coalesce(
            try_to_date(col('date_of_birth'),'yyyy-MM-dd'),
            try_to_date(col('date_of_birth'),'dd-MM-yyyy'),
            try_to_date(col('date_of_birth'),'yyyy/MM/dd'),
            try_to_date(col('date_of_birth'),'dd/MM/yyyy')
        )
)

customers_df = customers_df.withColumn(
    "registration_parsed",
    coalesce(
            try_to_date(col('registration_date'),'yyyy-MM-dd'),
            try_to_date(col('registration_date'),'dd-MM-yyyy'),
            try_to_date(col('registration_date'),'yyyy/MM/dd'),
            try_to_date(col('registration_date'),'dd/MM/yyyy')
        )
)

customers_df.display()

# COMMAND ----------

customers_df = customers_df.drop("date_of_birth", "registration_date") \
                           .withColumnRenamed("dob_parsed", "date_of_birth") \
                           .withColumnRenamed("registration_parsed", "registration_date")
     

customers_df.display()

# COMMAND ----------

customers_df = customers_df.withColumn(
    "gender",
    upper(col("gender"))
)

customers_df = customers_df.withColumn(
    "gender",
    when(col("gender") == "MALE", "M")
    .when(col("gender") == "FEMALE", "F")
    .otherwise("U")
)

# COMMAND ----------

customers_df.write.format("delta").mode("overwrite").saveAsTable("retail_project.silver.silver_customers")
     

# COMMAND ----------

# MAGIC %md
# MAGIC gold layer
# MAGIC

# COMMAND ----------

customers_df=spark.read.table("retail_project.silver.silver_customers")
orders_df=spark.read.table("retail_project.silver.silver_orders")

customers_df.display()
orders_df.display()

# COMMAND ----------

dim_customer = customers_df.select("customer_id",
                                   "customer_name",
                                   "email",
                                   "city",
                                   "state",
                                   "loyalty_points",
                                   "status"
)

dim_customer.display()

# COMMAND ----------

dim_customer.write.format("delta").mode("overwrite").saveAsTable("retail_project.gold.gold_dim_customer")

# COMMAND ----------

fact_sales = orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "left"
).select(
    orders_df.order_id,
    orders_df.order_date,
    orders_df.customer_id,
    customers_df.customer_name,
    orders_df.product_id,
    orders_df.product_name,
    orders_df.category,
    orders_df.quantity,
    orders_df.unit_price,
    orders_df.discount,
    orders_df.total_amount,
    customers_df.city,
    customers_df.state,
    customers_df.gender,
    customers_df.loyalty_points
)
 # fact_sales.display()


from pyspark.sql.functions import year , month , dayofmonth

order_enriched = fact_sales.\
    withColumn("Year",year("order_date")).\
    withColumn("Month",month("order_date")).\
    withColumn("Day",dayofmonth("order_date"))

order_enriched.display()


# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg, min,max

gold_df = order_enriched.groupBy(
    "product_id",
    "product_name",
    "customer_id",
    "customer_name",
    "category",
    "Year",
    "Month",
    "order_date",
    "gender",
    "city",
    "state",
    "loyalty_points"
).agg(
    sum("total_amount").alias("Total_revenue"),
    sum("quantity").alias("Total_quantity"),
    countDistinct("order_id").alias("Total_orders"),
    avg("total_amount").alias("Avg_revenue"),
    avg("quantity").alias("Avg_quantity"),
    avg("loyalty_points").alias("Avg_loyalty_points"),
    min("unit_price").alias("Min_price"),
    max("unit_price").alias("Max_price")
)
gold_df.display()
     

# COMMAND ----------

gold_df.write.format('delta').mode('overwrite').saveAsTable('retail_project.gold.gold_fact_Sales')
     
