import psycopg2
import json

# Load PostgreSQL configuration
with open('/Users/sushant-sharma/Documents/postgre/db_config.json', 'r') as f:
    db_config = json.load(f)

# Connect to PostgreSQL
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Query and display data
tables = ["average_counts_per_day", "status_counts", "user_type_metrics"]
for table in tables:
    print(f"\nData from {table}:")
    cur.execute(f"SELECT * FROM {table};")
    rows = cur.fetchall()
    for row in rows:
        print(row)

cur.close()
conn.close()
