import pyodbc
from faker import Faker
import time
import random
from datetime import datetime

# Database connection information
server = 'DESKTOP-R8U2TTC\SQLEXPRESS'
database = 'live_sales'
username = 'live_user'
password = 'test123'

# Establish a connection to the database
connection = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

# Create a cursor for executing SQL commands
cursor = connection.cursor()

fake = Faker()

while True:
    # Generate fake sales data
    product_name = fake.word()
    quantity = random.randint(1, 10)
    price = round(random.uniform(10, 100), 2)
    customer_name = fake.name()
    sale_datetime = datetime.now()  # Current datetime

    # Insert data into the sales table
    insert_query = f"INSERT INTO Sales (Product, Quantity, Price, Customer, SaleDatetime) VALUES (?, ?, ?, ?, ?)"
    cursor.execute(insert_query, (product_name, quantity, price, customer_name, sale_datetime))
    connection.commit()

    print(f"Inserted: {product_name}, {quantity}, ${price}, {customer_name}, {sale_datetime}")

    # Sleep for a while to control the data generation rate
    time.sleep(1)  # Adjust as needed
