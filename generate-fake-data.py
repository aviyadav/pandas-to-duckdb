import pandas as pd
import numpy as np
from faker import Faker
import random
import os
from datetime import datetime

fake = Faker()

Faker.seed(0)
random.seed(0)

def generate_fake_data(num_records):
    data = []
    start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-12-31', '%Y-%m-%d')

    for _ in range(num_records):
        order_id = _
        order_date = fake.date_between(start_date=start_date, end_date=end_date).strftime('%Y-%m-%d')
        customer_id = fake.random_int(min=100, max=999)
        customer_name = fake.name()
        product_id = fake.random_int(min=200, max=210)
        product_name = random.choice(
            ['Laptop', 'Smartphone', 'Desk', 'Chair', 'Monitor', 'Printer', 'Paper', 'Pen', 'Notebook', 'Coffee Maker'])
        category = 'Electronics' if product_name in ['Laptop', 'Smartphone', 'Monitor', 'Printer',
                                                     'Coffee Maker'] else 'Office'
        quantity = fake.random_int(min=1, max=10)
        price = round(random.uniform(1.99, 999.99), 2)
        total = round(price * quantity, 2)
        data.append(
            [order_id, order_date, customer_id, customer_name, product_id, product_name, category, quantity, price,
             total])
    return data

num_records = 10000000

data = generate_fake_data(num_records)

# Create DataFrame
columns = ['order_id', 'order_date', 'customer_id', 'customer_name', 'product_id', 'product_name', 'category', 'quantity', 'price', 'total']
df = pd.DataFrame(data, columns=columns)

df.to_csv('data/sales_data.csv')
print('CSV file with fake sales data has been created.')