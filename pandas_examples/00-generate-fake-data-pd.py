import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from polars_examples.utils import time_it

fake = Faker()
Faker.seed(0)
np.random.seed(0)

@time_it
def generate_fake_data(num_records):
    # Pre-generate unique values to sample from
    print("Generating reference data...")
    num_customers = 100_000
    customer_ids = np.random.randint(100, 1000, size=num_customers)
    customer_names = np.array([fake.name() for _ in range(num_customers)])
    
    # 10 products
    product_ids = np.arange(200, 210)
    product_names = np.array(['Laptop', 'Smartphone', 'Desk', 'Chair', 'Monitor', 'Printer', 'Paper', 'Pen', 'Notebook', 'Coffee Maker'])
    
    # Generate main data using numpy for speed
    print(f"Generating {num_records} rows...")
    order_ids = np.arange(num_records)
    
    # Dates
    start_date_np = np.datetime64('2023-01-01')
    days_range = 365
    random_days = np.random.randint(0, days_range, size=num_records)
    order_dates_np = start_date_np + random_days.astype('timedelta64[D]')
    
    # Customer selection
    customer_indices = np.random.randint(0, num_customers, size=num_records)
    final_customer_ids = customer_ids[customer_indices]
    final_customer_names = customer_names[customer_indices]
    
    # Product selection
    product_indices = np.random.randint(0, len(product_names), size=num_records)
    final_product_ids = product_ids[product_indices]
    final_product_names = product_names[product_indices]
    
    # Quantities and Prices
    quantities = np.random.randint(1, 11, size=num_records)
    prices = np.round(np.random.uniform(1.99, 999.99, size=num_records), 2)
    
    # Category logic
    electronics_items = {'Laptop', 'Smartphone', 'Monitor', 'Printer', 'Coffee Maker'}
    is_electronics = np.isin(final_product_names, list(electronics_items))
    categories = np.where(is_electronics, 'Electronics', 'Office')
    
    # Calculate total
    totals = np.round(prices * quantities, 2)
    
    print("Constructing DataFrame...")
    df = pd.DataFrame({
        'order_id': order_ids,
        'order_date': order_dates_np,
        'customer_id': final_customer_ids,
        'customer_name': final_customer_names,
        'product_id': final_product_ids,
        'product_name': final_product_names,
        'category': categories,
        'quantity': quantities,
        'price': prices,
        'total': totals
    })
    
    return df

num_records = 10_000_000

df = generate_fake_data(num_records)

df.to_csv('data/sales_data_pd.csv', index=False)
print('CSV file with fake sales data has been created.')