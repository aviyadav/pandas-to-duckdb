import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
df_polars = pl.read_parquet(polars_parquet_file_path)

start = timer()
result_polars = df_polars.filter(pl.col('product_id') == 200)
print("Polars: Data filtering", timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')
start = timer()
con.execute("CREATE table result_duckdb as SELECT * FROM sales_data  WHERE product_id = 200")
print("DuckDB: Data filtering", timer()-start,'seconds')

print()
print(result_polars.head(5))
print()
results_duckdb_df = con.execute('SELECT * FROM result_duckdb LIMIT 5').fetchdf()
print(results_duckdb_df.head(5))