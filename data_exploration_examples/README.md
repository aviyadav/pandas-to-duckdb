# MrBeast YouTube Stats — Data Exploration

A data exploration project built live on a Twitch stream, comparing four popular Python data tools — **pandas**, **polars**, **DataFusion**, and **DuckDB** — against the same dataset and the same analysis questions.

---

## Dataset

**MrBeast YouTube Stats (Daily)**
- Source: [Kaggle — robikscube/mrbeast-youtube-stats-daily](https://www.kaggle.com/datasets/robikscube/mrbeast-youtube-stats-daily)
- Contains daily snapshots of MrBeast's YouTube video statistics: view counts, like counts, comment counts, video metadata, and pull timestamps.

| File | Description |
|---|---|
| `data/MrBeast_youtube_stats.csv` | Raw CSV — one row per video per pull date |
| `data/MrBeast_youtube_stats_processed.parquet` | Processed Parquet — includes time-delta metrics |

---

## Project Structure

```
twitch-stream-project/
│
├── data/
│   ├── MrBeast_youtube_stats.csv                   # Raw data
│   └── MrBeast_youtube_stats_processed.parquet     # Processed data
│
├── download_data.py          # Downloads dataset from Kaggle via kagglehub
├── process_data.py           # Data processing pipeline (pandas)
├── process_data_pl.py        # Data processing pipeline (polars)
│
├── data_exploration_pandas.ipynb      # Exploration notebook — pandas
├── data_exploration_polars.ipynb      # Exploration notebook — polars
├── data_exploration_datafusion.ipynb  # Exploration notebook — DataFusion
├── data_exploration_duckdb.ipynb      # Exploration notebook — DuckDB
│
├── pyproject.toml            # Project dependencies (managed with uv)
└── README.md
```

---

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# Install dependencies
uv sync

# Activate the virtual environment
source .venv/bin/activate   # macOS / Linux
.venv\Scripts\activate      # Windows
```

**Python 3.13+** is required (see `.python-version`).

---

## Quickstart

### 1 — Download the data

```bash
python download_data.py
```

This uses `kagglehub` to download the dataset. You will need a [Kaggle API key](https://www.kaggle.com/docs/api) configured on your machine.

Copy the downloaded CSV into the `data/` directory:

```bash
cp <kaggle-download-path>/MrBeast_youtube_stats.csv data/
```

### 2 — Process the data

Run either processing script to produce the enriched Parquet file:

```bash
# pandas version
python process_data.py

# polars version
python process_data_pl.py
```

Both scripts perform the same steps:
1. Parse `pull_date` as a proper timestamp
2. Drop rows with missing `viewCount`, `likeCount`, or `commentCount`
3. Cast count columns to integers
4. Sort by `(id, pull_date)` and compute per-video diffs:
   - `time_since_last_pull` — time elapsed between pulls
   - `view_diff` — views gained since last pull
   - `like_diff` — likes gained since last pull
   - `comment_diff` — comments gained since last pull
5. Write output to `data/MrBeast_youtube_stats_processed.parquet`

### 3 — Run the notebooks

```bash
jupyter notebook
```

Open any of the four exploration notebooks from the Jupyter interface.

---

## Exploration Notebooks

All four notebooks answer the same questions using different tools:

| Notebook | Tool | Version |
|---|---|---|
| `data_exploration_pandas.ipynb` | pandas | ≥ 3.0.2 |
| `data_exploration_polars.ipynb` | polars | ≥ 1.39.3 |
| `data_exploration_datafusion.ipynb` | Apache DataFusion | ≥ 52.3.0 |
| `data_exploration_duckdb.ipynb` | DuckDB | ≥ 1.5.1 |

### Analysis performed

- **Duplicate title detection** — videos that have appeared under multiple titles
- **Pull frequency** — how many videos were captured per day
- **View & like trends** — time-series plots for individual videos
- **Engagement metrics** — like-to-view ratio over time
- **Normalised velocity** — views and likes gained per 12-hour period (`view_diff_norm`, `like_diff_norm`)
- **Top engagers** — videos with the highest normalised like ratio
- **Correlation** — viewCount vs likeCount for a specific video
- **Duration distribution** — histogram of video lengths
- **Latest snapshot** — most recent stats per video, ranked by comment ratio

---

## Tool Comparison Notes

| Feature | pandas | polars | DataFusion | DuckDB |
|---|---|---|---|---|
| Primary interface | DataFrame API | DataFrame API | SQL (`ctx.sql`) | SQL (`con.sql`) |
| CSV reading | `read_csv()` | `read_csv()` | Via polars → Arrow bridge | `FROM 'file.csv'` directly |
| Lazy evaluation | ❌ | ✅ (LazyFrame) | ✅ (query planner) | ✅ (query planner) |
| Column mutation | `df[col] = ...` | `with_columns()` | `CREATE OR REPLACE VIEW` | `CREATE OR REPLACE VIEW` |
| Filtering | `.query()` / boolean mask | `.filter()` | SQL `WHERE` | SQL `WHERE` |
| Group diffs | `.groupby().diff()` | `.diff().over()` | `LAG()` window function | `LAG()` window function |
| Window filter | subquery | `.filter()` | subquery `WHERE rn = 1` | `QUALIFY` clause |
| Pandas bridge | — | `.to_pandas()` | `.to_pandas()` | `.df()` |
| Case sensitivity | insensitive | sensitive | **sensitive** (quote camelCase) | insensitive |

---

## Dependencies

| Package | Purpose |
|---|---|
| `pandas` | DataFrame-based data manipulation |
| `polars` | Fast Rust-based DataFrame library |
| `datafusion` | Apache Arrow query engine (SQL) |
| `duckdb` | Embeddable analytical SQL engine |
| `pyarrow` | Arrow memory format, Parquet I/O |
| `matplotlib` | Plotting |
| `kagglehub` | Kaggle dataset download |
| `jupyter` | Interactive notebooks |