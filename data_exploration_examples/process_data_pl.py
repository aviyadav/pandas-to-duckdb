import polars as pl


def process_data(input_file, output_file):
    print(f"Reading {input_file}...")
    df = pl.read_csv(input_file)

    # Type casting and date conversion
    df = df.with_columns(
        pl.col("pull_date").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f%:z")
    )

    # Drop rows with missing values in key columns, then cast to Int64
    cols_to_check = ["viewCount", "likeCount", "commentCount"]
    df = df.drop_nulls(subset=cols_to_check)
    df = df.with_columns([pl.col(col).cast(pl.Int64) for col in cols_to_check])

    # Sort data for diff calculation
    df = df.sort(["id", "pull_date"])

    print("Computing metrics...")
    # Compute diffs within each video group using window expressions
    df = df.with_columns(
        [
            pl.col("pull_date").diff().over("id").alias("time_since_last_pull"),
            pl.col("viewCount").diff().over("id").alias("view_diff"),
            pl.col("likeCount").diff().over("id").alias("like_diff"),
            pl.col("commentCount").diff().over("id").alias("comment_diff"),
        ]
    )

    print(f"Saving to {output_file}...")
    df.write_parquet(output_file)
    print("Done!")


if __name__ == "__main__":
    import os

    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_csv = os.path.join(base_dir, "data", "MrBeast_youtube_stats.csv")
    output_parquet = os.path.join(
        base_dir, "data", "MrBeast_youtube_stats_processed_pl.parquet"
    )

    process_data(input_csv, output_parquet)
