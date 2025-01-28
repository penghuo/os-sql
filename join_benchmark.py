import time
import requests
import numpy as np
import csv

# OpenSearch REST API settings
BASE_URL = "http://localhost:9200"  # Replace with your OpenSearch endpoint
SQL_ENDPOINT = f"{BASE_URL}/_plugins/_sql"
HEADERS = {"Content-Type": "application/json"}

# Table sizes and limit sizes for testing
table_sizes = ["logs-181998", "logs-201998", "logs-241998"]  # Different left table sizes
limits = [200, 1000, 10000]  # Different LIMIT sizes

# Query templates
queries = {
    "basic_equality_join": """
        SELECT * FROM (SELECT CAST(_MAP['clientip'] AS VARCHAR(32)) AS clientip, CAST(_MAP['size'] AS int) AS size FROM "{table_size}") AS logs 
        LEFT JOIN (SELECT CAST(_MAP['clientip'] AS VARCHAR(32)) AS clientip, CAST(_MAP['request'] AS VARCHAR(32)) AS request FROM "dimension50k") AS dim 
        ON logs.clientip=dim.clientip LIMIT {limit_size}
    """,
    "filtered_join": """
        SELECT * FROM (SELECT CAST(_MAP['clientip'] AS VARCHAR(32)) AS clientip, CAST(_MAP['size'] AS int) AS size FROM "{table_size}") AS logs 
        LEFT JOIN (SELECT CAST(_MAP['clientip'] AS VARCHAR(32)) AS clientip, CAST(_MAP['request'] AS VARCHAR(32)) AS request FROM "dimension50k") AS dim 
        ON logs.clientip=dim.clientip WHERE logs.clientip='13.0.0.0' ORDER BY logs.size DESC LIMIT {limit_size}
    """,
    "order_join": """
        SELECT * FROM (SELECT CAST(_MAP['clientip'] AS VARCHAR(32)) AS clientip, CAST(_MAP['size'] AS int) AS size FROM "{table_size}") AS logs 
        LEFT JOIN (SELECT CAST(_MAP['clientip'] AS VARCHAR(32)) AS clientip, CAST(_MAP['request'] AS VARCHAR(32)) AS request FROM "dimension50k") AS dim 
        ON logs.clientip=dim.clientip WHERE logs.clientip='13.0.0.0' ORDER BY logs.size DESC LIMIT {limit_size}
    """
}

# Function to execute a single query via REST API
def execute_query(query):
    payload = {"query": query}
    start_time = time.time()
    response = requests.post(SQL_ENDPOINT, json=payload, headers=HEADERS)
    latency = time.time() - start_time

    # Handle errors
    if response.status_code != 200:
        raise Exception(f"Query failed: {response.json()}")
    
    response_data = response.json()
    return latency, response_data

# Function to calculate P90 latency
def calculate_p90(latencies):
    return np.percentile(latencies, 90)

# Performance testing
results = []
for query_name, query_template in queries.items():
    for table_size in table_sizes:
        for limit_size in limits:
            print(f"Testing query: {query_name}, table: {table_size}, limit: {limit_size}")
            
            # Format query with table size and limit size
            query = query_template.format(table_size=table_size, limit_size=limit_size)
            
            # Warm-up step
            print("  - Warm-up run...")
            try:
                _, _ = execute_query(query)
            except Exception as e:
                print(f"  - Warm-up failed: {e}")
                continue
            
            # Performance test: Execute query multiple times and capture latencies
            latencies = []
            for i in range(10):  # Execute each query 5 times
                print(f"  - Execution {i+1}...")
                try:
                    latency, response = execute_query(query)
                    latencies.append(latency)
                except Exception as e:
                    print(f"  - Execution failed: {e}")
                    latencies.append(float('inf'))
            
            # Calculate P90 latency
            if latencies:
                p90_latency = calculate_p90(latencies)
                rows_returned = len(response.get("datarows", [])) if response else 0
                results.append({
                    "query_name": query_name,
                    "table_size": table_size,
                    "limit_size": limit_size,
                    "p90_latency": p90_latency,
                    "rows_returned": rows_returned
                })

# Save results to a CSV file
output_file = "query_performance_results.csv"
csv_headers = ["query_name", "table_size", "limit_size", "p90_latency", "rows_returned"]

with open(output_file, mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=csv_headers)
    writer.writeheader()
    writer.writerows(results)

print(f"Results saved to {output_file}")