#import datetime
from json import loads
import csv
import os
from datetime import datetime
# from faker import Faker
from kafka import KafkaConsumer
import boto3

KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "github-repo"

s3 = boto3.client('s3', aws_access_key_id='<enteryouraccesskey', aws_secret_access_key='<enteryoursecretkey>')
bucket_name = '<add your bucket name>'

CSV_HEADERS = ['name', 'full_name', 'html_url', 'description', 'stars', 'forks', 'language', 'created_at', 'updated_at', 'open_issues']
CSV_DIRECTORY =  "/app/csv_data"
if not os.path.exists(CSV_DIRECTORY):
    os.makedirs(CSV_DIRECTORY)

print("Current working directory:", os.getcwd())

def generate_csv_filename():
    # Use a timestamp-based filename to ensure uniqueness
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    return os.path.join(CSV_DIRECTORY, f"stream_{timestamp}.csv")

def write_csv(csv_filename, data):
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(data)

def upload_to_s3(file_path, bucket, key):
    try:
        s3.upload_file(file_path, bucket, key)
        print(f"File uploaded successfully: {key}")
    except Exception as e:
        print(f"Error uploading file: {e}")

if __name__ == "__main__":
    consumer = KafkaConsumer(KAFKA_TOPIC, 
                            bootstrap_servers=KAFKA_SERVER, 
                            group_id='groupdId-919292',
                            auto_offset_reset='earliest',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))

    all_repo_data = []
    print("Consumer started")
    try:
        for message in consumer:
            repo_info = message.value

            for repo in repo_info:
                repo_data = {
                    'name': repo.get('name'),
                    'full_name': repo.get('full_name'),
                    'description': repo.get('description'),
                    'stars': repo.get('stargazers_count'),
                    'forks': repo.get('forks_count'),
                    'language': repo.get('language'),
                    'created_at': repo.get('created_at'),
                    'updated_at': repo.get('updated_at'),
                    'open_issues': repo.get('open_issues_count')
                }
                all_repo_data.append(repo_data)
            print(f"Writing {len(all_repo_data)} records to CSV.")
            csv_filename = generate_csv_filename()
            print(csv_filename)
            write_csv(csv_filename, all_repo_data)
            s3_key = f"csv_data/{os.path.basename(csv_filename)}"
            upload_to_s3(csv_filename, bucket_name, s3_key)
            all_repo_data = []
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        # Close the consumer
        consumer.close()
    