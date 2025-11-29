import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_polars.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
df_polars = pl.read_parquet(polars_parquet_file_path)

start = timer()
result_polars = df_polars.pivot(values='total', index='order_date', on='product_name', aggregate_function='sum').fill_null(0)
print("Polars: Data pivoting", timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')

pivot_query = """
Create table result_duckdb as SELECT
    order_date,
    SUM(CASE WHEN product_name = 'Laptop' THEN total ELSE 0 END) AS Laptop,
    SUM(CASE WHEN product_name = 'Smartphone' THEN total ELSE 0 END) AS Smartphone,
    SUM(CASE WHEN product_name = 'Coffee Maker' THEN total ELSE 0 END) AS Coffee_Maker,
    SUM(CASE WHEN product_name = 'Paper' THEN total ELSE 0 END) AS Paper,
    SUM(CASE WHEN product_name = 'Monitor' THEN total ELSE 0 END) AS Monitor,
    SUM(CASE WHEN product_name = 'Notebook' THEN total ELSE 0 END) AS Notebook,
    SUM(CASE WHEN product_name = 'Chair' THEN total ELSE 0 END) AS Chair,
    SUM(CASE WHEN product_name = 'Desk' THEN total ELSE 0 END) AS Desk,
    SUM(CASE WHEN product_name = 'Pen' THEN total ELSE 0 END) AS Pen,
    SUM(CASE WHEN product_name = 'Printer' THEN total ELSE 0 END) AS Printer
FROM sales_data
GROUP BY order_date
ORDER BY order_date
"""
start = timer()
result_duckdb = con.execute(pivot_query).fetchdf()
print("DuckDB: Data pivoting",timer()-start,'seconds')

print()
print(result_polars.head(5))
print()
df = con.execute('SELECT * FROM result_duckdb LIMIT 5').fetchdf()
print(df)