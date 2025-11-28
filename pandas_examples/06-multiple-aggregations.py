import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
df_pandas = pd.read_parquet(pandas_parquet_file_path)

start = timer()
result_pandas = df_pandas.groupby('customer_id').agg({
    'total': ['min', 'max', 'mean'],
    'quantity': ['sum', 'count']
}).reset_index()
print("pandas: multiple aggregations", timer()-start,'seconds')
result_pandas.columns = ['customer_id', 'total_min', 'total_max', 'total_mean', 'quantity_sum', 'quantity_count']

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
print(result_pandas)
print()
print(result_duckdb)