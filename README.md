# Github Trending Repositories

## Architecture

![Flink-weather-streaming](link for the architecture image)

## Overview

The project simulates continuously reads weather data from a Kafka topic, calculates the average temperature for each city every minute, and stores the results in a PostgreSQL database.

Here's a detailed breakdown of the project:

**1. Kafka Producer:**

* **gen_data()** function requests github event data every 20 seconds and sends it to the 'github-events' topic in Kafka.
* **Language:** Python

**2. Flink Processor:**

* **GithubTrendJob.java** is the main entry point for the Flink application.
* **GithubEventData class** represents a github event data with id, repo, and other attributes.
* **GithubEventDeserializationSchema** is a custom deserialization schema that reads JSON-formatted github event data from Kafka.
* **Main() function** creates a Flink streaming environment and reads event data from the 'github-events' topic using KafkaSource.
* **Aggregation** is performed to sum the number of events based on the repo key
* **SlidingProcessingTimeWindows** is an aggregation function that calculates the count of events for each repo every 2 minutes considering the last 10 minutes data.
* **repoAndValueStream** is a DataStream containing the repo and total events count for each repo.
* **MyProcessWindowFunction** is a custom processing function that sorts the aggregated stream every 2 minutes and retrieves the top 10 repos
* **topTrendingRepos** is a DataStream containing the top 10 repositories with highest number of activities
* **githubSink** is the KafkaSink created to publish the top trending repo information to Kafka cluster topic 'trending-github-repos'
* **Language:** Java

**3. Github Consumer:**

* **KafkaConsumer** consumes the final top 10 trending repos from the topic 'trending-github-repos' 
* **Language:** Python

Overall, the project utilizes Kafka for real-time data ingestion, Flink for data processing and aggregation.

## Instructions for Building and Running a Flink Application with Kafka and PostgreSQL

This document provides comprehensive instructions for building and running a Flink application that ingests github data from Kafka, processes it, to find the trending repositories and sends the results to another Kafka topic for further application usage.

### Prerequisites:

Docker and Docker Compose installed
Familiarity with Flink, Kafka

### Build and Run:

a. Build the Kafka Producer Docker image:
* cd kafka-producer 
* docker build -t kafka-producer .

b. Build the Flink Processor Docker image:
* cd flink-processor
* mvn clean package
* docker build -t flink-processor .

b. Build the Github Consumer Docker image:
* cd github-consumer
* docker build -t github-consumer .

c. Start the Docker containers:
* docker-compose up -d

d. Verify from logs of github-consumer container
