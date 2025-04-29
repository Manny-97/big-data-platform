import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    datediff,
    dayofweek,
    expr,
    lit,
    max,
    month,
    rand,
    round,
)
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import to_date, when, year


def create_spark_session(app_name="Test the Cluster"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def generate_synthetic_sales_data(spark, n_records=100_000_000):
    ship_modes = ["First Class", "Second Class", "Standard Class", "Same Day"]
    regions = ["East", "West", "Central", "South"]
    categories = ["Furniture", "Office Supplies", "Technology"]
    sub_categories = ["Chairs", "Tables", "Binders", "Phones", "Accessories"]
    states = ["California", "Texas", "New York", "Florida", "Illinois", "Washington"]
    postal_codes = [str(90000 + i) for i in range(100)]

    def list_to_expr_str(lst):
        return ",".join([f"'{x}'" for x in lst])

    ship_modes_str = list_to_expr_str(ship_modes)
    postal_codes_str = list_to_expr_str(postal_codes)
    regions_str = list_to_expr_str(regions)
    categories_str = list_to_expr_str(categories)
    sub_categories_str = list_to_expr_str(sub_categories)
    states_str = list_to_expr_str(states)

    df = spark.range(n_records).withColumnRenamed("id", "row_id")

    df = (
        df.withColumn(
            "Order_Date", to_date(expr("date_add('2022-01-01', int(rand() * 1000))"))
        )
        .withColumn("Ship_Date", expr("date_add(Order_Date, int(rand() * 7))"))
        .withColumn(
            "Ship_Mode",
            expr(
                f"""element_at(array({ship_modes_str}),
            int(rand() * {len(ship_modes)}) + 1)"""
            ),
        )
        .withColumn(
            "Postal_Code",
            expr(
                f"""element_at(array({postal_codes_str}),
            int(rand() * {len(postal_codes)}) + 1)"""
            ),
        )
        .withColumn(
            "Region",
            expr(
                f"""element_at(array({regions_str}),
            int(rand() * {len(regions)}) + 1)"""
            ),
        )
        .withColumn("Product_Reference", concat_ws("-", lit("PRD"), col("row_id")))
        .withColumn(
            "Category",
            expr(
                f"""element_at(array({categories_str}),
            int(rand() * {len(categories)}) + 1)"""
            ),
        )
        .withColumn(
            "Sub_Category",
            expr(
                f"""element_at(array({sub_categories_str}),
            int(rand() * {len(sub_categories)}) + 1)"""
            ),
        )
        .withColumn("Sales", round(rand() * 1000, 2))
        .withColumn("Quantity", (rand() * 10).cast("int") + 1)
        .withColumn("Profit", round(rand() * 500 - 100, 2))
        .withColumn(
            "State",
            expr(f"element_at(array({states_str}), int(rand() * {len(states)}) + 1)"),
        )
    )

    return df


def enrich_and_clean_data(df):
    df = (
        df.withColumn("shipping_delay", datediff("Ship_Date", "Order_Date"))
        .dropna(subset=["Order_Date", "Sales", "Profit"])
        .filter((col("Profit") > 0) & (col("shipping_delay") <= 31))
        .withColumn("order_year", year("Order_Date"))
        .withColumn("order_month", month("Order_Date"))
        .withColumn("order_day_of_week", dayofweek("Order_Date"))
        .withColumn(
            "profit_margin",
            when(col("Sales") != 0, col("Profit") / col("Sales")).otherwise(0),
        )
    )

    return df


def compute_analytics(df):
    print("Latest order date:")
    df.agg(max("Order_Date").alias("latest_order_date")).show()

    print("\nTop Performing Categories:")
    df.groupBy("Category").agg(
        _sum("Profit").alias("total_profit"), _sum("Sales").alias("total_sales")
    ).orderBy(col("total_profit").desc()).show()

    print("\nSales by Region and State:")
    df.groupBy("Region", "State").agg(
        _sum("Sales").alias("total_sales"), _sum("Profit").alias("total_profit")
    ).orderBy("Region", col("total_sales").desc()).show()

    print("\nAverage Shipping Delay by Ship Mode:")
    df.groupBy("Ship_Mode").agg(
        avg("shipping_delay").alias("avg_shipping_delay")
    ).orderBy("avg_shipping_delay").show()


def main():
    start_time = time.time()
    spark = create_spark_session("Test the Cluster")

    print("Generating synthetic sales data...")
    sales_df = generate_synthetic_sales_data(spark)

    print(f"\nTotal partitions created Before: {sales_df.rdd.getNumPartitions()}")
    print("Sample records:")
    sales_df.show(5)

    print(f"Total rows: {sales_df.count()}")

    print("Cleaning and enriching data...")
    sales_df = enrich_and_clean_data(sales_df)

    print("Running analytics...")
    compute_analytics(sales_df)

    spark.stop()
    end_time = time.time()
    duration = end_time - start_time
    print(f"\nTotal execution time: {duration:.2f} seconds")


if __name__ == "__main__":
    main()
