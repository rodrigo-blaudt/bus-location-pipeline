import io
import json
import os
import snappy

from datetime import datetime
from minio import Minio
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


CONFIG = {
    'CONSUMER_GROUP': os.getenv('CONSUMER_GROUP', 'valid-data-to-storage-consumer-group'),
    'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'SCHEMA_REGISTRY_URL': os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
    'INVALID_DATA_TOPIC': os.getenv('INVALID_DATA_TOPIC', 'invalid_data_topic'),
    'VALID_DATA_TOPIC': os.getenv('VALID_DATA_TOPIC', 'valid_data_topic'),
    'SCHEMA_PATH': os.getenv('SCHEMA_PATH', 'services/kafka/schemas/api_data.avsc'),
    'BATCH_SIZE': os.getenv('BATCH_SIZE', 18000),
    'BATCH_TIMEOUT': os.getenv('BATCH_TIMEOUT', 60),
    'MINIO_ENDPOINT': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
    'MINIO_ACCESS_KEY': os.getenv('MINIO_ACCESS_KEY', 'CqI3RE1f9GEPelJu'),
    'MINIO_SECRET_KEY': os.getenv('MINIO_SECRET_KEY', '2uv67vAHkhTqFAiToaOxVsQ7SidlsOUA'),
    'MINIO_BUCKET': os.getenv('MINIO_BUCKET', 'kafka-data')
}


def initialize_minio():
    try:
        minio_client = Minio(
            CONFIG['MINIO_ENDPOINT'],
            access_key=CONFIG['MINIO_ACCESS_KEY'],
            secret_key=CONFIG['MINIO_SECRET_KEY'],
            secure=False
        )

        return minio_client
    except Exception as e:
        print(f"Failed to initialize MinIO client: {e}")
        raise


def upload_to_minio(minio_client, data, minio_key):
    try:
        data_io = io.BytesIO(data)

        result = minio_client.put_object(
            CONFIG['MINIO_BUCKET'],
            minio_key,
            data_io,
            len(data)
        )
        return result
    except Exception as e:
        print(f"MinIO upload failed: {e}")
        return None


def create_object_key(batch_time):
    timestamp_str = batch_time.strftime("%Y%m%d_%H%M%S")
    return f"data/year={batch_time.year}/month={batch_time.month:02d}/day={batch_time.day:02d}/batch_{timestamp_str}.json.snappy"


def flush_batch(batch, last_flush, consumer, minio_client, reason=None):
    try:
        if reason:
            print(f"Flushing batch due to {reason}")

        json_lines = '\n'.join([json.dumps(item[1]) for item in batch])
        compressed_data = snappy.compress(json_lines.encode('utf-8'))
        minio_key = create_object_key(last_flush)

        if upload_to_minio(minio_client, compressed_data, minio_key):
            offsets = {}
            for msg, _ in batch:
                tp = TopicPartition(msg.topic(), msg.partition())
                offsets[tp] = max(offsets.get(tp, -1), msg.offset() + 1)

            consumer.commit(offsets=[TopicPartition(tp.topic, tp.partition, offset)
                            for tp, offset in offsets.items()])
            print(f"Uploaded {len(batch)} messages to {CONFIG['MINIO_ENDPOINT']}/{CONFIG['MINIO_BUCKET']}/{minio_key}")
            return [], datetime.now()

        print("MinIO upload failed")
        return batch, last_flush

    except Exception as e:
        print(f"Batch processing failed: {e}")
        return batch, last_flush


def main():
    minio_client = initialize_minio()

    with open(CONFIG['SCHEMA_PATH'], 'r') as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': CONFIG['SCHEMA_REGISTRY_URL']})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    consumer = Consumer({
        'bootstrap.servers': CONFIG['KAFKA_BOOTSTRAP_SERVERS'],
        'group.id': CONFIG['CONSUMER_GROUP'],
        'auto.offset.reset': "earliest",
        'enable.auto.commit': False
    })
    consumer.subscribe([CONFIG['VALID_DATA_TOPIC']])

    batch = []
    last_flush = datetime.now()
    batch_size = CONFIG['BATCH_SIZE']
    batch_timeout = CONFIG['BATCH_TIMEOUT']

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                if len(batch) > 0 and (datetime.now() - last_flush).seconds >= batch_timeout:
                    batch, last_flush = flush_batch(
                        batch, last_flush, consumer, minio_client,
                        reason=f"timeout ({batch_timeout} seconds)"
                    )
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if data is not None:
                    batch.append((msg, data))
            except Exception as e:
                print(f"Deserialization error: {e}")
                continue

            if len(batch) >= batch_size or (datetime.now() - last_flush).seconds >= batch_timeout:
                batch, last_flush = flush_batch(batch, last_flush, consumer, minio_client)

    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        if len(batch) > 0:
            print("Flushing remaining messages...")
            flush_batch(batch, last_flush, consumer, minio_client)
        consumer.close()


if __name__ == '__main__':
    main()
