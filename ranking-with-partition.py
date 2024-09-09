import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
df_pandas = pd.read_parquet(pandas_parquet_file_path)

start = timer()
df_pandas['total_rank'] = df_pandas.groupby('order_date')['total'].rank(method='dense')
print("Pandas: ranking with partition",timer()-start,'seconds')

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
print(df_pandas[['order_id', 'order_date', 'customer_id', 'total', 'total_rank']])
print()
print(result_duckdb[['order_id', 'order_date', 'customer_id', 'total', 'total_rank']])