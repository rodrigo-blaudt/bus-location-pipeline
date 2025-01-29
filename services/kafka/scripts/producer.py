import logging
import os
import json
import httpx
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

CONFIG = {
    'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'SCHEMA_REGISTRY_URL': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
    'VALID_RECORDS_TOPIC': os.getenv('VALID_RECORDS_TOPIC', 'valid_data_topic'),
    'INVALID_RECORDS_TOPIC': os.getenv('INVALID_RECORDS_TOPIC', 'invalid_data_topic'),
    'API_URL': os.getenv('API_URL', 'https://temporeal.pbh.gov.br/?param=D'),
    'SCHEMA_PATH': os.getenv('SCHEMA_PATH', 'services/kafka/schemas/api_data.avsc'),
    'RUN_INTERVAL_SECONDS': os.getenv('RUN_INTERVAL_SECONDS', 20)
}

schema_registry_conf = {'url': CONFIG['SCHEMA_REGISTRY_URL']}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

try:
    with open(CONFIG['SCHEMA_PATH'], 'r') as f:
        schema_str = f.read()
except FileNotFoundError:
    print(f"Schema file {CONFIG['SCHEMA_PATH']} not found")
    exit(1)
except Exception as e:
    print(f"Error reading schema file: {e}")
    exit(1)

avro_serializer = AvroSerializer(schema_registry_client, schema_str)

headers = {
    'authority': 'temporeal.pbh.gov.br',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'accept-language': 'en-GB,en;q=0.7',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
}

producer_valid_data_config = {
    'bootstrap.servers': CONFIG['KAFKA_BOOTSTRAP_SERVERS'],
    'value.serializer': avro_serializer,
    'retries': 5,
    'message.timeout.ms': 30000
}
producer_valid_data = SerializingProducer(producer_valid_data_config)


def json_serializer(data, is_key):
    return json.dumps(data).encode('utf-8')


producer_invalid_data_config = {
    'bootstrap.servers': CONFIG['KAFKA_BOOTSTRAP_SERVERS'],
    'value.serializer': json_serializer,
    'retries': 5,
    'message.timeout.ms': 30000
}
producer_invalid_data = SerializingProducer(producer_invalid_data_config)

valid_records_topic = CONFIG['VALID_RECORDS_TOPIC']
invalid_records_topic = CONFIG['INVALID_RECORDS_TOPIC']


def fetch_data_from_api():
    with httpx.Client(http2=True, headers=headers, follow_redirects=True) as client:
        response = client.get(CONFIG['API_URL'])

        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError:
                return {"content": response.text}
        else:
            raise Exception(f'Failed to fetch data: {response.status_code} - {response.text}')


def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        pass
        # logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')


def validate_record(record: dict) -> bool:
    required_fields = ["EV", "HR", "LT", "LG", "NV", "VL", "NL", "DG", "SV", "DT"]
    return all(field in record for field in required_fields)


def send_data_topic(producer, topic_name, records):
    for record in records:
        try:
            producer.produce(
                topic=topic_name,
                value=record,
                on_delivery=delivery_report
            )
            producer.poll(0)
        except Exception as e:
            logging.error(f"Error producing record: {e}")
    try:
        producer.flush(timeout=10)
    except Exception as e:
        logging.error(f"Flush failed: {e}")


def send_to_kafka(data):
    if not isinstance(data, list):
        data = [data]

    valid_records = [r for r in data if validate_record(r)]
    logging.info(f"Collected {len(valid_records)} valid records")

    invalid_records = [r for r in data if not validate_record(r)]
    logging.info(f"Collected {len(invalid_records)} invalid records")

    logging.info(f"Sending records to topic: {valid_records_topic}")
    send_data_topic(producer_valid_data, valid_records_topic, valid_records)

    logging.info(f"Sending records to DLQ topic: {invalid_records_topic}")
    send_data_topic(producer_invalid_data, invalid_records_topic, invalid_records)


if __name__ == '__main__':
    try:
        while True:
            try:
                data = fetch_data_from_api()
                send_to_kafka(data)
                logging.info('Data successfully sent to Kafka.')
            except Exception as e:
                logging.error(f'Error: {e}')

            time.sleep(CONFIG['RUN_INTERVAL_SECONDS'])

    except KeyboardInterrupt:
        logging.info('Script stopped by user')
    finally:
        logging.info('Script exiting')
