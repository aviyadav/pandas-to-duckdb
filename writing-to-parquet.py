import pandas as pd
import duckdb
from timeit import default_timer as timer

start = timer()
df_pandas = pd.read_csv('data/sales_data.csv')
df_pandas.to_parquet('data/sales_data_pandas.parquet', engine='pyarrow', index=False)
print("Pandas: Writing data to Parquet", timer()-start,'seconds')

start = timer()
con = duckdb.connect(database=':memory:')
csv_file_path = 'data/sales_data.csv'
start = timer()
con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM read_csv_auto('{csv_file_path}')
''')
con.execute("COPY sales_data TO 'data/sales_data_duckdb.parquet' (FORMAT PARQUET)")
print("DuckDB: Writing data to Parquet", timer()-start,'seconds')


# !dir "d:\sales_data\*.parquet"