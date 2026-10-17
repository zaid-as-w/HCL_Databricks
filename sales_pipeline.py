# Databricks notebook source
#%sql
CREATE VOLUME IF NOT EXISTS
workspace.default.smartsales_files;

# COMMAND ----------

catalog = "workspace"  # Replace with your catalog name
schema = "default"
volume = "smartsales_files"

file_path = (
    f"/Volumes/{catalog}/{schema}/{volume}/"
    "databricks_hackathon_ecommerce_orders.csv"
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.smartsales")

print("Project schema:", f"{catalog}.smartsales")
print("CSV path:", file_path)

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)

display(raw_df)
print("Raw record count:", raw_df.count())
raw_df.printSchema()

# COMMAND ----------

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.smartsales.bronze_orders")
)

# COMMAND ----------

clean_df = (
    raw_df
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn(
        "product_name",
        F.trim(F.col("product_name"))
    )
    .withColumn(
        "order_date",
        F.to_date("order_date", "yyyy-MM-dd")
    )
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn(
        "discount_pct",
        F.col("discount_pct").cast("double")
    )
    .withColumn(
        "payment_method",
        F.coalesce(F.col("payment_method"), F.lit("Unknown"))
    )
    .withColumn(
        "order_status",
        F.coalesce(F.col("order_status"), F.lit("Unknown"))
    )
    .withColumn(
        "data_quality_issue",
        F.when(
            F.col("order_date").isNull(), "Invalid date"
        ).when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            "Invalid quantity"
        ).when(
            F.col("unit_price").isNull() | (F.col("unit_price") < 0),
            "Invalid price"
        ).when(
            F.col("discount_pct").isNull()
            | (F.col("discount_pct") < 0)
            | (F.col("discount_pct") > 1),
            "Invalid discount"
        ).otherwise("Valid")
    )
)

display(clean_df)

# COMMAND ----------

valid_df = clean_df.filter(
    F.col("data_quality_issue") == "Valid"
)

invalid_df = clean_df.filter(
    F.col("data_quality_issue") != "Valid"
)

print("Valid records:", valid_df.count())
print("Invalid records:", invalid_df.count())

display(invalid_df)

# COMMAND ----------

(
    valid_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.smartsales.silver_orders")
)

(
    invalid_df.write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.smartsales.data_quality_issues")
)

# COMMAND ----------

sales_df = (
    valid_df
    .filter(F.col("order_status") == "Delivered")
    .withColumn(
        "gross_amount",
        F.col("quantity") * F.col("unit_price")
    )
    .withColumn(
        "net_revenue",
        F.round(
            F.col("gross_amount")
            * (1 - F.col("discount_pct")),
            2
        )
    )
)

display(sales_df)

# COMMAND ----------

category_sales = (
    sales_df
    .groupBy("product_category")
    .agg(
        F.sum("net_revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("delivered_orders")
    )
    .orderBy(F.desc("total_revenue"))
)

display(category_sales)

# COMMAND ----------

city_sales = (
    sales_df
    .groupBy("city")
    .agg(
        F.sum("net_revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("delivered_orders")
    )
    .orderBy(F.desc("total_revenue"))
)

display(city_sales)

# COMMAND ----------

monthly_sales = (
    sales_df
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("month"))
    .agg(F.round(F.sum("net_revenue"), 2).alias("total_revenue"))
    .orderBy("month")
)

display(monthly_sales)

# COMMAND ----------

for df, table in [
    (category_sales, "gold_category_sales"),
    (city_sales, "gold_city_sales"),
    (monthly_sales, "gold_monthly_sales")
]:
    (
        df.write.format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.smartsales.{table}")
    )

# COMMAND ----------

#%sql
SELECT
    product_category,
    ROUND(
        SUM(quantity * unit_price * (1 - discount_pct)),
        2
    ) AS total_revenue
FROM workspace.smartsales.silver_orders
WHERE order_status = 'Delivered'
   AND data_quality_issue = 'Valid'
GROUP BY product_category
ORDER BY total_revenue DESC;