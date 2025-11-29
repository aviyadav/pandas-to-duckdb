# DuckDB with Pandas and Polars

This project demonstrates the usage of DuckDB, Pandas, and Polars for data processing tasks. It includes examples for generating fake data, reading/writing files, and performing various data transformations.

## Project Structure

- `pandas_examples/`: Contains scripts demonstrating Pandas usage.
- `polars_examples/`: Contains scripts demonstrating Polars usage.
- `data/`: Directory where generated data files are stored.

## Setup

1.  **Install dependencies**:
    Ensure you have `uv` installed, then run:
    ```bash
    uv sync
    ```

## Usage

### Generating Fake Data

To generate 10 million rows of fake sales data, you can use either the Pandas or Polars script. Both have been optimized for performance.

**Polars:**
```bash
uv run python polars_examples/00-generate-fake-data-pl.py
```

**Pandas:**
```bash
uv run python pandas_examples/00-generate-fake-data-pd.py
```

### Running Examples

You can run other examples similarly. For instance, to run the pivoting example in Polars:

```bash
uv run python polars_examples/07-data-pivoting.py
```

## Performance Benchmarks

The data generation scripts have been optimized to generate 10 million rows in approximately **20 seconds** on a standard machine, down from significantly higher times with standard Python loops.

## References

- [Moving from Pandas to DuckDB](https://ai.gopubby.com/moving-from-pandas-to-duckdb-3ba10903ec13)