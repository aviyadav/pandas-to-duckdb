import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_polars.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
start = timer()
df_polars = pl.read_parquet(polars_parquet_file_path)
print("Polars: Read from 10M record Parquet file", timer()-start, 'seconds')


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
print(df_polars.head(5))
print()
df = con.execute('SELECT * FROM sales_data LIMIT 5').fetchdf()
print(df.head())