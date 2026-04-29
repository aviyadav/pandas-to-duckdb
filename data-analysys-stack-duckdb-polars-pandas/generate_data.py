"""
Script to generate 10 million rows of event data in parquet format.
Batched into multiple files and processed using multiprocessing.
"""
import os
import concurrent.futures
import polars as pl
from faker import Faker
import random
import time

TOTAL_ROWS = 10_000_000
BATCH_SIZE = 100_000
OUTPUT_DIR = "data"
NUM_BATCHES = TOTAL_ROWS // BATCH_SIZE

def generate_batch(batch_idx: int) -> str:
    """Generates a single batch of fake event data and writes it to a parquet file."""
    fake = Faker()
    
    event_type_choices = ["login", "logout", "purchase", "view", "click"]
    
    # Pre-generate lists for Polars DataFrame
    user_ids = [fake.uuid4() for _ in range(BATCH_SIZE)]
    event_types = random.choices(event_type_choices, k=BATCH_SIZE)
    amounts = [round(random.uniform(1.0, 1000.0), 2) if et == "purchase" else 0.0 for et in event_types]
    created_ats = [fake.date_time_between(start_date="-1y", end_date="now") for _ in range(BATCH_SIZE)]
    
    df = pl.DataFrame({
        "user_id": pl.Series(user_ids, dtype=pl.String),
        "event_type": pl.Series(event_types, dtype=pl.String),
        "amount": pl.Series(amounts, dtype=pl.Float64),
        "created_at": pl.Series(created_ats, dtype=pl.Datetime("us"))
    })
    
    file_path = os.path.join(OUTPUT_DIR, f"events_{batch_idx:04d}.parquet")
    # Write to parquet
    df.write_parquet(file_path)
    
    return file_path

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Starting generation of {TOTAL_ROWS} rows...")
    print(f"Batch size: {BATCH_SIZE}, Total batches: {NUM_BATCHES}")
    print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    
    start_time = time.time()
    
    # Using ProcessPoolExecutor to utilize multiple CPU cores
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(generate_batch, i): i for i in range(NUM_BATCHES)}
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
                completed += 1
                if completed % 10 == 0 or completed == NUM_BATCHES:
                    print(f"Progress: {completed}/{NUM_BATCHES} batches processed.")
            except Exception as exc:
                batch_idx = futures[future]
                print(f"Batch {batch_idx} generated an exception: {exc}")

    duration = time.time() - start_time
    print(f"Finished generating data in {duration:.2f} seconds.")

if __name__ == "__main__":
    main()
