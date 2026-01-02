This project implements an end-to-end retail data lakehouse using Azure Databricks, Delta Lake, and the Medallion Architecture (Bronze, Silver, Gold).

The pipeline ingests customers, products, and orders data from Azure Data Lake Storage Gen2 (ADLS) using Databricks Auto Loader (Structured Streaming) and transforms it into analytics-ready Gold tables.

The project is designed following production data engineering best practices, including:

- Streaming ingestion
- Incremental processing
- Slowly Changing Dimensions (SCD Type 2)
- Data quality checks
- Performance optimization
- CI/CD readiness