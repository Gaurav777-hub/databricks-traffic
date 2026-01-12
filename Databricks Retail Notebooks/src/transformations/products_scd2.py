from pyspark.sql.functions import (
    col, row_number, lead, max as spark_max, lit
    )
from pyspark.sql.window import Window

def prepare_products_scd2(df):
    """
    Prepares SCD Type 2 records for products using event-time logic.
    Returns a dataframe ready for delta merge
    """

    #1. Deduplicate by business key + effective_from
    dedup_window = (
        Window
        .partitionBy("product_id", "effective_from")
        .orderBy("ingestion_ts")
        )
    
    df_deduped = (
        df.withColumn("rn",row_number().over(dedup_window))
            .filter(col("rn") == 1)
            .drop("rn")
        )
    

    #2 Compute effective_to using event-time ordering
    timeline_window = (
        Window
        .partitionBy("product_id")
        .orderBy("effective_from")
        )
    
    df_timed =  (
        df_deduped.withColumn(
            "effective_to",
            lead("effective_from").over(timeline_window)
            )          
    )

    #3 Identify latest version per record
    latest = (
        df_timed
        .groupBy("product_id")
        .agg(
            spark_max("effective_from").alias("max_effective_from"))
    )

    df_final = (
        df_timed.alias("s")
        .join(
            latest.alias("m"),
            col("s.product_id") == col("m.product_id") &
            col("s.effective_from") == col("m.max_effective_from"),
            "left"            
            )
        .select(
        col("s.product_id").alias("product_id"),
        col("s.product_name"),
        col("s.category"),
        col("s.brand"),
        col("s.price"),
        col("s.currency"),
        col("s.is_active"),
        col("s.effective_from"),
        col("s.ingestion_ts"),
        col("s.source_file_path"),
        col("m.max_effective_from").isNotNull().alias("is_current")
        )
        .drop("max_effective_from")
    )
    
    return df_final