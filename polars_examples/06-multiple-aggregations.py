import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_polars.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
df_polars = pl.read_parquet(polars_parquet_file_path)

start = timer()
result_polars = df_polars.group_by('customer_id').agg([
    pl.col('total').min().alias('total_min'),
    pl.col('total').max().alias('total_max'),
    pl.col('total').mean().alias('total_mean'),
    pl.col('quantity').sum().alias('quantity_sum'),
    pl.col('quantity').count().alias('quantity_count')
])
print("polars: multiple aggregations", timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')

query = """
SELECT customer_id,
       MIN(total) AS total_min,
       MAX(total) AS total_max,
       AVG(total) AS total_mean,
       SUM(quantity) AS quantity_sum,
       COUNT(quantity) AS quantity_count
FROM sales_data
GROUP BY customer_id
"""
start = timer()
result_duckdb = con.execute(query).fetchdf()
print("DuckDB: multiple aggregations", timer()-start,'seconds')

print()
print(result_polars)
print()
print(result_duckdb)