import polars as pl
import duckdb
from timeit import default_timer as timer

# File path to your Parquet file
polars_parquet_file_path = 'data//sales_data_pandas.parquet'

duckdb_parquet_file_path = 'data//sales_data_duckdb.parquet'

# Polars first
df_polars = pl.read_parquet(polars_parquet_file_path)

start = timer()
result_polars = df_polars.group_by('order_date').agg(
    pl.col('total').sum().alias('total')
).sort('order_date')

# Calculate the LAG and LEAD values for daily_totals
result_polars = result_polars.with_columns([
    pl.col('total').shift(1).alias('total_lag'),
    pl.col('total').shift(-1).alias('total_lead')
])

# Calculate the percent change
result_polars = result_polars.with_columns([
    ((pl.col('total') - pl.col('total_lag')) / pl.col('total_lag') * 100).alias('percent_change_from_lag'),
    ((pl.col('total') - pl.col('total_lead')) / pl.col('total_lead') * 100).alias('percent_change_from_lead')
])
print("Polars: Analytic Windowing functions", timer()-start,'seconds')

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
print(result_polars)
print()
df = con.execute('SELECT * FROM result_duckdb').fetchdf()
print(df)