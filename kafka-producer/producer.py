import csv
import time
from datetime import datetime
from kafka import KafkaProducer
import os
from metrics_logger import MetricsLogger
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError


def wait_for_topic(bootstrap_servers, topic_name):
    print(f"Waiting for topic '{topic_name}' to exist on {bootstrap_servers}...")

    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            topics = admin_client.list_topics()
            admin_client.close()

            if topic_name in topics:
                print(f"Topic '{topic_name}' exists. Proceeding...")
                return
            else:
                print(f"Topic '{topic_name}' not found. Retrying in 2 seconds...")
        except KafkaError as e:
            print(f"Kafka error while checking topic: {e}. Retrying in 2 seconds...")

        time.sleep(2)


topic_name = 'taxi-location'
kafka_servers = ['kafka:9092', 'kafka2:9092', 'kafka3:9092']

wait_for_topic(kafka_servers, topic_name)

print("Initializing Kafka")
producer = KafkaProducer(
    bootstrap_servers=kafka_servers, 
    compression_type='gzip'
)

metrics_logger = MetricsLogger(service_name="kafka-producer", log_interval_seconds=5)


def on_send_success(data, start_time):
    latency = time.time() - start_time
    metrics_logger.record_success(latency)
    #print(f"Sent to {data.topic} partition {data.partition} offset {data.offset} | Latency: {latency:.4f}s")

def on_send_error(excp):
    metrics_logger.record_error(excp)
    print(f"Failed to send message: {excp}")

def send_batch(batch):
    print(f"Sending batch with {len(batch)} records...")
    for row in batch:
        epoch_time = str(time.time()) #time at which msg is produced

        message = ','.join(row + [epoch_time]).encode('utf-8')

        send_start = time.time()
        # Send the message to the Kafka location 'taxi-location'
        ack = producer.send(topic_name, message)
        ack.add_callback(lambda data, start=send_start: on_send_success(data, start))
        ack.add_errback(on_send_error)
        print(f"Sent: {message.decode('utf-8')}")
    # Ensure all messages are sent before proceeding
    producer.flush()


base_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(base_dir, 'data', 'combined_taxis_data.csv')

with open(csv_path, 'r') as f:
    reader = csv.reader(f)
    header = next(reader)  # Skip the CSV header

    current_batch = []
    prev_timestamp = None

    for row in reader:
        try:
            # Parse timestamp from second column (index 1)
            current_timestamp = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"Skipping row due to timestamp error: {row}")
            continue

        if prev_timestamp is None:
            prev_timestamp = current_timestamp
            current_batch.append(row)
        else:
            if current_timestamp == prev_timestamp:
                # Same timestamp, add to current batch
                current_batch.append(row)
            else:
                # Different timestamp, send current batch
                send_batch(current_batch)

                # Calculate delay between timestamps and sleep accordingly
                delay = (current_timestamp - prev_timestamp).total_seconds()
                print(
                    f"Sleeping for {delay} seconds between {prev_timestamp} -> {current_timestamp}")
                if delay > 0:
                    time.sleep(delay)

                # Reset batch with the new row
                current_batch = [row]
                prev_timestamp = current_timestamp

    # Send any remaining batch after loop ends
    if current_batch:
        send_batch(current_batch)
