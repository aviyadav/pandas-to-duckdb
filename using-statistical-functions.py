import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
df_pandas = pd.read_parquet(pandas_parquet_file_path)

start = timer()
result_pandas = df_pandas.groupby('product_id').apply(lambda x: x['quantity'].corr(x['price']))
print("Pandas: quantity/price correlation",timer()-start,'seconds')

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
print(result_pandas)
print()
print(result_duckdb)