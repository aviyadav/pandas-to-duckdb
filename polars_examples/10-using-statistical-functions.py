import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_polars.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
df_polars = pl.read_parquet(polars_parquet_file_path)

start = timer()
result_polars = df_polars.group_by('product_id').agg(
    pl.corr('quantity', 'price').alias('correlation')
)
print("Polars: quantity/price correlation",timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')

query = """
SELECT product_id,
           (COUNT(*) * SUM(quantity * price) - SUM(quantity) * SUM(price)) /
           (SQRT(COUNT(*) * SUM(quantity * quantity) - SUM(quantity) * SUM(quantity)) *
            SQRT(COUNT(*) * SUM(price * price) - SUM(price) * SUM(price))) as correlation
    FROM sales_data
    GROUP BY product_id
"""
start = timer()
result_duckdb = con.execute(query).fetchdf()
print("DuckDB: quantity/price correlation", timer()-start,'seconds')

print()
print(result_polars)
print()
print(result_duckdb)