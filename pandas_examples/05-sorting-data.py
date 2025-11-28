import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
df_pandas = pd.read_parquet(pandas_parquet_file_path)

start = timer()
result_pandas = df_pandas.sort_values(['customer_name','product_id','order_id'])
print("Pandas: Data sorting", timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')
start = timer()
con.execute('create table result_duckdb as SELECT * FROM sales_data order by customer_name,product_id,order_id')
print("DuckDB: Data sorting", timer()-start,'seconds')

print()
print(result_pandas.head(5))
print()
results_duckdb_df = con.execute('SELECT * FROM result_duckdb LIMIT 5').fetchdf()
print(results_duckdb_df.head(5))