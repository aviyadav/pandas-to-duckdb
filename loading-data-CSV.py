import pandas as pd
import duckdb
from timeit import default_timer as timer

# pandas
start = timer()
df_pandas = pd.read_csv('data/sales_data.csv')
print("Pandas: Create dataframe from 10M record CSV file", timer()-start,'seconds')

# Duckdb
con = duckdb.connect(database=':memory:')
csv_file_path = 'data/sales_data.csv'
start = timer()
con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM read_csv_auto('{csv_file_path}')
''')
print("DuckDB: Create table from 10M record CSV file", timer()-start,'seconds')

print()
print(df_pandas.head(5))
print()
df = con.execute('SELECT * FROM sales_data LIMIT 5').fetchdf()
print(df.head(5))