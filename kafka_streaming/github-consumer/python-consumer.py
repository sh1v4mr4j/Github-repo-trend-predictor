from json import loads
import csv
import os
import time
from datetime import datetime

from kafka import KafkaConsumer

# Define constants for Kafka server and topic
KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "github-repo"
# Maximum number of CSV files to retain to avoid over usage of memory.
MAX_CSV_FILES = 10

# Define CSV headers for the output file
CSV_HEADERS = ['Id','Name','FullName','HtmlUrl','Description','Language',
               'CreatedAt','UpdatedAt','PushedAt','OpenIssuesCount',
               'ForksCount','StargazersCount','WatchersCount','Size',
               'OwnerLogin','OwnerType','License']

# Define the directory to store CSV files
CSV_DIRECTORY =  "/app/csv_data/"
if not os.path.exists(CSV_DIRECTORY):
    os.makedirs(CSV_DIRECTORY)

print("Current working directory:", os.getcwd())

# Generate a unique CSV filename based on the current timestamp
def generate_csv_filename():
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    return os.path.join(CSV_DIRECTORY, f"stream_{timestamp}.csv")

# Write the provided data to a CSV file with the specified filename
def write_csv(csv_filename, data):
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(data)

# Track the time taken by the conumer file to run completely.
def log_consumer_time(time_diff):
    trace_file = "app/consumer_time_trace.csv"
    # Check if the file exists to write headers only once
    file_exists = os.path.exists(trace_file)
    with open(trace_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([time_diff])

# Manage the number of CSV files in the directory
# Delete the oldest csv file in the directory if the file count increases from 10 in the directory.
def manage_csv_files():
    # Get all the files in the CSV directory
    files = [f for f in os.listdir(CSV_DIRECTORY) if f.endswith('.csv')]
    
    # Sort the files by creation time (oldest first)
    files.sort(key=lambda x: os.path.getctime(os.path.join(CSV_DIRECTORY, x)))

    # If there are more than 10 files, delete the oldest one
    if len(files) > MAX_CSV_FILES:
        oldest_file = files[0]
        os.remove(os.path.join(CSV_DIRECTORY, oldest_file))
        print(f"Deleted old CSV file: {oldest_file}")

if __name__ == "__main__":
    
    # Initialize the Kafka consumer to read messages from the specified topic
    consumer = KafkaConsumer(KAFKA_TOPIC, 
                            bootstrap_servers=KAFKA_SERVER, 
                            group_id='groupdId-919292',
                            auto_offset_reset='earliest',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))
    # Initialize a list to store all repository data
    all_repo_data = []

    print("Consumer started")

    try:    
        # Continuously listen for messages from Kafka
        for message in consumer:
            message_startTime = time.time()
            repo_info = message.value

            for repo in repo_info:
                # Create a dictionary to store relevant repository data
                repo_data = {
                    'Id': repo['id'],
                    'Name': repo['name'],
                    'FullName': repo['full_name'],
                    'HtmlUrl': repo['html_url'],
                    'Description': repo['description'],
                    'Language': repo['language'],
                    'CreatedAt': repo['created_at'],
                    'UpdatedAt': repo['updated_at'],
                    'PushedAt': repo['pushed_at'],
                    'OpenIssuesCount': repo['open_issues_count'],
                    'ForksCount': repo['forks_count'],
                    'StargazersCount': repo['stargazers_count'],
                    'WatchersCount': repo['watchers_count'],
                    'Size': repo['size'],
                    'OwnerLogin': repo['owner']['login'],
                    'OwnerType': repo['owner']['type'],
                    'License': repo['license']['name'] if repo['license'] else 'NA'
                }

                # Add the repository data to the list
                all_repo_data.append(repo_data)
            print(f"Writing {len(all_repo_data)} records to CSV.")

            csv_filename = generate_csv_filename()
            print(csv_filename)
            write_csv(csv_filename, all_repo_data)

            manage_csv_files()
            
            # Reset the list for the next batch of data
            all_repo_data = []

            message_endTime = time.time()
            time_diff = message_endTime - message_startTime
            log_consumer_time(time_diff)
        
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        # Close the consumer
        consumer.close()