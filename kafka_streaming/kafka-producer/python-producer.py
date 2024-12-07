import os
import csv
import time 
import schedule 
import requests 
from json import dumps 
from datetime import datetime, timedelta 

from kafka import KafkaProducer 

# Define constants for Kafka server and topic
KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "github-repo"
GITHUB_EVENTS_URL = "https://api.github.com/"

# Initialize last fetched timestamp as two minutes ago in ISO 8601 format
last_fetched_timestamp = (datetime.utcnow() - timedelta(minutes=2)).isoformat() + "Z"  # ISO 8601 format
print(last_fetched_timestamp)  

# Define query parameters for GitHub API
# Query for repositories pushed after the last fetched timestamp with more than 0 stars
query = f"pushed:>{last_fetched_timestamp} stars:>0" 
# Initialize page number for pagination
page = 1
# Number of results per page
per_page = 100

# Construct the GitHub API URL for searching repositories
GITHUB_REPOS_URL = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&page={page}&per_page={per_page}"

def log_producer_time(time_diff):
    trace_file = "app/producer_time_trace.csv"
    # Check if the file exists to write headers only once
    file_exists = os.path.exists(trace_file)
    with open(trace_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([time_diff])

def gen_data():
  # Start time tracking for producer execution
  producer_startTime = time.time()

  global last_fetched_timestamp
  
  # Initialize Kafka producer with specified server and JSON serializer
  producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda x:dumps(x).encode('utf-8')) 
  
  try:
    # Make a GET request to the GitHub API to fetch repositories
    response = requests.get(GITHUB_REPOS_URL, headers={"If-Modified-Since": last_fetched_timestamp})
    
    # Check if the request was successful
    if response.status_code == 200:
      repos = response.json()     
          
      # Update the last_fetched_timestamp
      if 'items' in repos:
        repo_list = repos['items']  # List of repositories
        # Send the repository list to the Kafka topic
        producer.send(KAFKA_TOPIC, repo_list)
        # Count the number of repositories fetched
        repo_count = len(repos['items'])
        print(f"Number of repositories fetched: {repo_count}")
        
    elif response.status_code == 304:
      print("No new events since the last fetch.")

    # Handle other HTTP errors  
    else:
      print(f"Error: {response.status_code}, {response.text}")
  
  # Catch any exceptions during the request
  except Exception as e:
    print(f"Error: {e}")
  
  # Ensure all messages are sent to Kafka
  producer.flush()

 # End time tracking for producer execution
  producer_endTime = time.time()
  time_diff = producer_endTime-producer_startTime
  log_producer_time(time_diff)

  print(f"Running the entire Kafka Producer takes: {str(producer_endTime-producer_startTime)} seconds")

if __name__ == "__main__":
  # Call the data generation function
  gen_data()
  # Schedule the gen_data function to run every 2 minutes
  schedule.every(2).minutes.do(gen_data) 
  
  while True:
    # Run scheduled tasks
    schedule.run_pending()
    # Sleep for a short duration to prevent high CPU usage
    time.sleep(0.5)