import pytest
from pyspark.sql import SparkSession
 
from src.transformations.products_scd2 import prepare_products_scd2

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-tests")
        .getOrCreate()
    )


def test_scd2_multiple_versions(spark):
    data =[
        ("P001", 69999, "2023-01-01", "2026-01-01"),
        ("P001", 72999, "2023-05-15", "2026-01-02"),
        ("P001", 74999, "2023-06-01", "2026-01-03"),
    ]

    df = spark.createDataFrame(
        data,
        ["product_id", "price", "effective_from", "ingestion_ts"]
    )

    result = prepare_products_scd2(df).orderBy("effective_from").collect()

    assert len(result) == 3

    assert result[0]["effective_to"] == "2023-05-15"
    assert result[1]["effective_to"] == "2023-06-01"
    assert result[2]["effective_to"] is None
    assert result[2]["is_current"] is True

def test_deduplication(spark):
    data = [
        ("P001", 69999, "2023-01-01", "2026-01-01"),
        ("P001", 69999, "2023-01-01", "2026-01-02"),  # replay
    ]

    df = spark.createDataFrame(
        data,
        ["product_id", "price", "effective_from", "ingestion_ts"]
    )

    result = prepare_products_scd2(df).collect
    
    assert len(result) == 1

def test_late_arriving_event(spark):
    data = [
        ("P001", 74999, "2023-06-01", "2026-01-03"),
        ("P001", 72999, "2023-05-15", "2026-01-04"),  # late
    ]

    df = spark.createDataFrame(
        data,
        ["product_id", "price", "effective_from", "ingestion_ts"]
    )

    result = prepare_products_scd2(df).orderBy("effective_from").collect()

    assert result[-1].is_current is True
    assert result[-1].price == 74999
