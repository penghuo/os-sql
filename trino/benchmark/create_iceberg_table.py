#!/usr/bin/env python3
"""
Convert ClickBench hits.parquet to Iceberg table format using PySpark.

Optimizations for ClickBench query set:
  - Parquet file format (columnar, compressed, best for analytics)
  - Sorted by (CounterID, EventDate) — many queries filter on these
  - No partitioning (single-node PoC, full scans dominate)
  - Zstd compression (better ratio than snappy, fast enough)
  - Row group size 128MB (good for predicate pushdown)

Usage:
  python3 trino/benchmark/create_iceberg_table.py [warehouse_dir]

The warehouse_dir should match what Trino's Iceberg catalog uses.
Default: /tmp/trino-iceberg-warehouse (matches TrinoEngine temp dir pattern)
"""

import os
import sys
import time

from pyspark.sql import SparkSession

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PARQUET_FILE = os.path.join(DATA_DIR, "hits.parquet")

# Warehouse location — must match Trino's iceberg catalog warehouse
WAREHOUSE = sys.argv[1] if len(sys.argv) > 1 else "/tmp/iceberg-clickbench-warehouse"

if not os.path.exists(PARQUET_FILE):
    print(f"ERROR: {PARQUET_FILE} not found")
    print(f"Download: wget -O {PARQUET_FILE} https://datasets.clickhouse.com/hits_compatible/hits.parquet")
    sys.exit(1)

print("=== ClickBench Parquet → Iceberg Conversion ===")
print(f"Source:    {PARQUET_FILE}")
print(f"Warehouse: {WAREHOUSE}")
print()

# Iceberg Spark runtime — downloads automatically via Maven
ICEBERG_VERSION = "1.5.2"
ICEBERG_SPARK_JAR = f"org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:{ICEBERG_VERSION}"

spark = (
    SparkSession.builder
    .appName("ClickBench-Iceberg-Setup")
    .master("local[*]")
    .config("spark.driver.memory", "16g")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
    .config("spark.jars.packages", ICEBERG_SPARK_JAR)
    # Parquet/Iceberg write settings
    .config("spark.sql.parquet.compression.codec", "zstd")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

print("Step 1: Reading hits.parquet...")
t0 = time.time()
df = spark.read.parquet(PARQUET_FILE)
row_count = df.count()
t1 = time.time()
print(f"  Read {row_count:,} rows, {len(df.columns)} columns in {t1-t0:.1f}s")
print(f"  Schema sample: {', '.join(f'{c.name}:{c.dataType.simpleString()}' for c in df.schema[:10])}...")

# Print date/timestamp column types for verification
for col in df.schema:
    if col.name.lower() in ('eventdate', 'eventtime', 'clienteventtime', 'localeventtime'):
        print(f"  {col.name}: {col.dataType}")

print()

# Cast Short (int16) columns to Integer (int32) to avoid Iceberg writer ClassCastException.
# The ClickBench Parquet file uses int16 for many columns, but Iceberg's Parquet writer
# expects Integer for IntegerType columns.
from pyspark.sql.types import ShortType
from pyspark.sql.functions import col

cast_exprs = []
for field in df.schema:
    if isinstance(field.dataType, ShortType):
        cast_exprs.append(col(field.name).cast("int").alias(field.name))
    else:
        cast_exprs.append(col(field.name))

df = df.select(*cast_exprs)
print(f"  Cast {sum(1 for f in df.schema if isinstance(f.dataType, ShortType))} ShortType → IntegerType")

print("Step 2: Creating Iceberg namespace...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.clickbench")

print("Step 3: Writing Iceberg table (sorted by CounterID, EventDate)...")
print("  This will take several minutes for 100M rows...")
t0 = time.time()

# Drop if exists (for re-runs)
spark.sql("DROP TABLE IF EXISTS iceberg.clickbench.hits")

# Write as Iceberg table — no global sort to avoid OOM on 100M rows
# sortWithinPartitions is cheap (no shuffle) and provides some data clustering
(
    df.sortWithinPartitions("CounterID", "EventDate")
    .writeTo("iceberg.clickbench.hits")
    .tableProperty("format-version", "2")
    .tableProperty("write.parquet.compression-codec", "zstd")
    .tableProperty("write.parquet.row-group-size-bytes", str(128 * 1024 * 1024))
    .tableProperty("write.metadata.compression-codec", "gzip")
    .tableProperty("write.target-file-size-bytes", str(512 * 1024 * 1024))
    .create()
)

t1 = time.time()
print(f"  Done in {t1-t0:.1f}s")

print()
print("Step 4: Verifying Iceberg table...")
count = spark.sql("SELECT COUNT(*) FROM iceberg.clickbench.hits").collect()[0][0]
print(f"  Row count: {count:,}")

# Show table metadata
files = spark.sql("SELECT * FROM iceberg.clickbench.hits.files").count()
print(f"  Data files: {files}")

# Show file sizes
file_stats = spark.sql("""
    SELECT COUNT(*) as num_files,
           SUM(file_size_in_bytes) / (1024*1024*1024) as total_gb,
           AVG(file_size_in_bytes) / (1024*1024) as avg_mb,
           SUM(record_count) as total_records
    FROM iceberg.clickbench.hits.files
""").collect()[0]
print(f"  Files: {file_stats['num_files']}, Total: {file_stats['total_gb']:.2f} GB, Avg: {file_stats['avg_mb']:.1f} MB")

# Show schema
print()
print("Step 5: Schema of Iceberg table:")
spark.sql("DESCRIBE iceberg.clickbench.hits").show(200, truncate=False)

print()
print(f"=== Iceberg table created at {WAREHOUSE}/clickbench/hits ===")
print(f"  Warehouse: {WAREHOUSE}")
print(f"  Table: iceberg.clickbench.hits")
print(f"  Rows: {count:,}")
print(f"  Format: Parquet + Zstd compression")
print(f"  Sorted by: CounterID, EventDate")

spark.stop()
