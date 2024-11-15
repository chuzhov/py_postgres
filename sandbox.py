import os
from dotenv import load_dotenv
import pandas as pd
from postgre import PostgresHandler

load_dotenv()

dbname = os.getenv('PG_NAME')
user = os.getenv('PG_USER')
password = os.getenv('PG_PASSWORD')
host = os.getenv('PG_HOST')
port = os.getenv('PG_PORT')

# Initialize the handler
db = PostgresHandler(
    dbname,
    user,
    password,
    host,
    port
)

# Create a table
db.create_table('users', {
    'id': 'SERIAL PRIMARY KEY',
    'name': 'VARCHAR(100)',
    'email': 'VARCHAR(100)',
    'age': 'INTEGER'
})

# # Insert a record and get its ID
# new_id = db.insert_record('users', {
#     'name': 'John Doe',
#     'email': 'john@example.com',
#     'age': 30
# })
# print(f"Inserted record ID: {new_id}")

# Bulk insert and get all IDs
new_ids = db.bulk_insert('users', [
    {'name': 'Robert Johnson II', 'email': 'rj2@example.com', 'age': 18},
    {'name': 'William Smith II', 'email': 'ws2@example.com', 'age': 15}
])
print(f"Inserted record IDs: {new_ids}")

# # Update records and get count of affected rows
# affected = db.update_records(
#     'users',
#     data={'age': 31},
#     conditions={'name': 'John Doe'}
# )
# print(f"Updated {affected} records")

results = db.select_records(table_name='users', order_by='age DESC', output_type='pd.df')

print(results)

raw_query = '''
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';'''
results = db.execute_raw_query(raw_query)

for result in results:
    print(result)

raw_query = '''
SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'users';'''
results = db.execute_raw_query(raw_query)

for result in results:
    print(result)