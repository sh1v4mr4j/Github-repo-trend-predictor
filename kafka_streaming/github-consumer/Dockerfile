FROM python:3.8-slim

COPY requirements.txt .

RUN set -ex; \
  	pip install --no-cache-dir -r requirements.txt


#RUN ./wait-for-it.sh

# Copy resources
WORKDIR /

COPY wait-for-it.sh wait-for-it.sh

#Windows configuration
RUN apt-get update && apt-get install -y dos2unix && dos2unix wait-for-it.sh

RUN chmod +x wait-for-it.sh

ADD python-consumer.py .

CMD ./wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- python -u python-consumer.py