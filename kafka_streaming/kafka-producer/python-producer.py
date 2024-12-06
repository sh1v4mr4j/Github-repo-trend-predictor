#import datetime
import time
import schedule
import requests
from json import dumps
from datetime import datetime, timedelta

from kafka import KafkaProducer

KAFKA_SERVER = "kafka:9092"
KAFKA_TOPIC = "github-repo"
GITHUB_EVENTS_URL = "https://api.github.com/"

# Initialize last fetched timestamp as one minute ago

last_fetched_timestamp = (datetime.utcnow() - timedelta(minutes=2)).isoformat() + "Z"  # ISO 8601 format
print(last_fetched_timestamp)

query = f"pushed:>{last_fetched_timestamp} stars:>0"
page = 1
per_page = 100

GITHUB_REPOS_URL = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&page={page}&per_page={per_page}"

def gen_data():
  global last_fetched_timestamp
  
  producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda x:dumps(x).encode('utf-8')) 
  
  try:
    response = requests.get(GITHUB_REPOS_URL, headers={"If-Modified-Since": last_fetched_timestamp})
    if response.status_code == 200:
      repos = response.json()

      
          
      # Update the last_fetched_timestamp
      if 'items' in repos:
        repo_list = repos['items']  # List of repositories
        producer.send(KAFKA_TOPIC, repo_list)
        repo_count = len(repos['items'])
        print(f"Number of repositories fetched: {repo_count}")
        
    elif response.status_code == 304:
      print("No new events since the last fetch.")
      
    else:
      print(f"Error: {response.status_code}, {response.text}")
    
  except Exception as e:
    print(f"Error: {e}")

  producer.flush()

if __name__ == "__main__":
  gen_data()
  schedule.every(2).minutes.do(gen_data) # Respect GitHub API rate limits
  
  while True:
    schedule.run_pending()
    time.sleep(0.5)