import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
df_pandas = pd.read_parquet(pandas_parquet_file_path)

start = timer()
result_pandas = df_pandas.pivot_table(values='total', index='order_date', columns='product_name', aggfunc='sum', fill_value=0)
print("Pandas: Data pivoting", timer()-start,'seconds')

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
print(result_pandas.head(5))
print()
df = con.execute('SELECT * FROM result_duckdb LIMIT 5').fetchdf()
print(df)