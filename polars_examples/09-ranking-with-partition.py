import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_polars.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
df_polars = pl.read_parquet(polars_parquet_file_path)

start = timer()
df_polars = df_polars.with_columns(
    pl.col('total').rank(method='dense').over('order_date').alias('total_rank')
)
print("Polars: ranking with partition",timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')

query = """
SELECT *,
       DENSE_RANK() OVER (PARTITION BY order_date ORDER BY total) AS total_rank
FROM sales_data
order by order_id
"""
start = timer()
result_duckdb = con.execute(query).fetchdf()
print("DuckDB: ranking with partition", timer()-start,'seconds')

print()
print(df_polars.select(['order_id', 'order_date', 'customer_id', 'total', 'total_rank']))
print()
print(result_duckdb[['order_id', 'order_date', 'customer_id', 'total', 'total_rank']])