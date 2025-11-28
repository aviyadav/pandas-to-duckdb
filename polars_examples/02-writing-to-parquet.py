import polars as pl
import duckdb
from timeit import default_timer as timer

start = timer()
df_polars = pl.read_csv('data/sales_data_pl.csv')
df_polars.write_parquet('data/sales_data_pandas.parquet')
print("Polars: Writing data to Parquet", timer()-start,'seconds')

start = timer()
con = duckdb.connect(database=':memory:')
csv_file_path = 'data/sales_data_pl.csv'
start = timer()
con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM read_csv_auto('{csv_file_path}')
''')
con.execute("COPY sales_data TO 'data/sales_data_duckdb.parquet' (FORMAT PARQUET)")
print("DuckDB: Writing data to Parquet", timer()-start,'seconds')


# !dir "d:\sales_data\*.parquet"