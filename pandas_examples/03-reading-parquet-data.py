import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
start = timer()
df_pandas = pd.read_parquet(pandas_parquet_file_path)
print("Pandas: Read from 10M record Parquet file", timer()-start, 'seconds')


# DuckDB
# Create a connection to DuckDB (in-memory database for this example)
con = duckdb.connect(database=':memory:')

start = timer()
# Create a DuckDB table from the Parquet file
con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')
print("DuckDB: Read from 10M record Parquet file", timer()-start, 'seconds')


print()
print(df_pandas.head(5))
print()
df = con.execute('SELECT * FROM sales_data LIMIT 5').fetchdf()
print(df.head())