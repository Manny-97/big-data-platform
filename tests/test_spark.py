import pytest
import logging
from pipeline import (
    create_spark_session,
    generate_synthetic_sales_data,
    enrich_and_clean_data,
    compute_analytics,
)


def test_create_spark_session():
    spark = create_spark_session("Test App")
    assert spark is not None
    assert spark.sparkContext.appName == "Test App"


def test_generate_synthetic_sales_data_shape(spark):
    df = generate_synthetic_sales_data(spark, n_records=1000)
    assert df.count() == 1000
    expected_columns = {
        "Order_Date",
        "Ship_Date",
        "Ship_Mode",
        "Postal_Code",
        "Region",
        "Product_Reference",
        "Category",
        "Sub_Category",
        "Sales",
        "Quantity",
        "Profit",
        "State",
    }
    assert expected_columns.issubset(set(df.columns))


def test_enrich_and_clean_data_filtering(spark):
    df = generate_synthetic_sales_data(spark, n_records=1000)
    enriched_df = enrich_and_clean_data(df)

    # Check that required columns were added
    assert "shipping_delay" in enriched_df.columns
    assert "profit_margin" in enriched_df.columns

    # Check that all profits are > 0 and delay <= 31
    assert enriched_df.filter("Profit <= 0").count() == 0
    assert enriched_df.filter("shipping_delay > 31").count() == 0


def test_compute_analytics_execution(spark, caplog):
    caplog.set_level(logging.INFO)

    df = generate_synthetic_sales_data(spark, n_records=1000)
    enriched_df = enrich_and_clean_data(df)

    # This should run without errors and print output
    compute_analytics(enriched_df)

    # Now the logs should be available
    assert "Top Performing Categories" in caplog.text
