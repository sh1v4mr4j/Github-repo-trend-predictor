# Github Trending Repositories

## Overview

The project simulates and continuously reads trending github repository data from a Kafka topic, every 2 minutes and is consumed by the Kafka consumer.

Here's a detailed breakdown of the project:

**1. Kafka Producer:**

* **gen_data()** function requests github event data every 20 seconds and sends it to the 'github-events' topic in Kafka.
* **Language:** Python

**2. Github Consumer:**

* **KafkaConsumer** consumes the trending github repositories from the topic 'github-repo' 
* **Language:** Python

Overall, the project utilizes Kafka for real-time data ingestion.

## Instructions for Building and Running a Kafka

This document provides comprehensive instructions for building and running an application that ingests github data from Kafka, processes it, to find the trending repositories and sends the results to another Kafka topic for further application usage.

### Prerequisites:

Docker and Docker Compose installed
Familiarity with Kafka, Python, Docker, Machine Learning Models
Basic knowledge of GitHub

### Build and Run:

a. Build the Kafka Producer Docker image:
* cd kafka-producer 
* docker build -t kafka-producer .

b. Build the Github Consumer Docker image:
* cd github-consumer
* docker build -t github-consumer .

c. Start the Docker containers:
* docker-compose up -d

d. Verify from logs of github-consumer container
