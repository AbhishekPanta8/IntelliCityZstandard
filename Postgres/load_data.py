import psycopg2
import json

# Load PostgreSQL configuration
with open('/Users/sushant-sharma/Documents/postgre/db_config.json', 'r') as f:
    db_config = json.load(f)

# Establish a database connection
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Insert sample data into tables
cur.execute("""
INSERT INTO average_counts_per_day (day_of_week, average_counts) VALUES
('Monday', 22.5), ('Tuesday', 23.0), ('Wednesday', 24.1);
""")

cur.execute("""
INSERT INTO status_counts (status, count) VALUES
('raw', 443407), ('processed', 20000);
""")

cur.execute("""
INSERT INTO user_type_metrics (user_type, active_user_count) VALUES
('pedestrian', 119117), ('car', 39947), ('bicycle', 80398);
""")

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()

print("Sample data inserted successfully.")
