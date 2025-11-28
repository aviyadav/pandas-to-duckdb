import pandas as pd
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
pandas_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Pandas first
df_pandas = pd.read_parquet(pandas_parquet_file_path)

start = timer()
result_pandas = df_pandas.groupby('order_date')['total'].sum().reset_index()

# Calculate the LAG and LEAD values for daily_totals
result_pandas['total_lag'] = result_pandas['total'].shift(1)
result_pandas['total_lead'] = result_pandas['total'].shift(-1)

# Calculate the percent change
result_pandas['percent_change_from_lag'] = (result_pandas['total'] - result_pandas['total_lag']) / daily_totals['total_lag'] * 100
result_pandas['percent_change_from_lead'] = (result_pandas['total'] - result_pandas['total_lead']) / daily_totals['total_lead'] * 100
print("Pandas: Analytic Windowing functions", timer()-start,'seconds')

# DuckDB
con = duckdb.connect(database=':memory:')

con.execute(f'''
    CREATE TABLE sales_data AS
    SELECT * FROM parquet_scan('{duckdb_parquet_file_path}')
''')

query = """
WITH daily_totals AS (
    SELECT
        order_date,
        SUM(total) AS total
    FROM sales_data
    GROUP BY order_date
),
lagged_totals AS (
    SELECT
        order_date,
        total,
        LAG(total, 1) OVER (ORDER BY order_date) AS total_lag,
        LEAD(total, 1) OVER (ORDER BY order_date) AS total_lead
    FROM daily_totals
)
SELECT
    order_date,
    total,
    total_lag,
    total_lead,
    ((total - total_lag) / total_lag) * 100 AS percent_change_from_lag,
    ((total - total_lead) / total_lead) * 100 AS percent_change_from_lead
FROM lagged_totals
"""

start = timer()
result_duckdb = con.execute(query).fetchdf()
print("DuckDB: Analytic Windowing functions", timer()-start,'seconds')

print()
print(print(result_pandas))
print()
df = con.execute('SELECT * FROM result_duckdb').fetchdf()
print(df)