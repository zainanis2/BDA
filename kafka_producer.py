from kafka import KafkaProducer
import csv
import time

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send data to Kafka
with open('ecommerce.csv', 'r') as file:
    reader = csv.reader(file)
    header = next(reader)  # Skip the header
    for row in reader:
        producer.send('ecommerce-data', value=','.join(row).encode('utf-8'))
        print(f"Sent: {row}")

producer.close()
