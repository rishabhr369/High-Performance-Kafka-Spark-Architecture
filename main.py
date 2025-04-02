import threading
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import uuid
import time
import random
import json

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 10  # Increased for higher parallelism
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer_conf = {
    "bootstrap.servers": KAFKA_BROKERS,
    "queue.buffering.max.messages": 1000000,
    "queue.buffering.max.kbytes": 1048576,  # 1GB
    "batch.num.messages": 10000,
    "linger.ms": 100,
    "acks": 1,
    "compression.type": "lz4",  # faster than gzip
    "enable.idempotence": False,
}

producer = Producer(producer_conf)


def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR,
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic_name}' created successfully")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
        else:
            logger.info(f"Topic '{topic_name}' already exists")
    except Exception as e:
        logger.error(f"Error Creating Topic: {e}")


def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1,100000)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time()),
        merchantId=random.choice(["merchant1", "merchant2", "merchant3"]),
        transactionType=random.choice(["purchase", "refund"]),
        location=f"location {random.randint(1,50)}",
        paymentMethod=random.choice(["credit_card", "paypal", "bank_transfer"]),
        isInternational=random.choice(["True", "False"]),
        currency=random.choice(["USD", "EUR", "GBP"]),
    )


def delivery_report(err, msg):
    if err is not None:
        logger.warning(f"Delivery failed for record {msg.key()}: {err}")
    # else:
    #     logger.info(f"Record {msg.key()} successfully produced")


def produce_transaction(thread_id, batch_size=1000):

    while True:
        for _ in range(batch_size):
            transaction = generate_transaction()
            try:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=transaction["userId"],
                    value=json.dumps(transaction).encode("utf-8"),
                    on_delivery=delivery_report,
                )
            except BufferError:
                # Back off if buffer is full
                producer.poll(0.1)
        # Allow background delivery
        producer.poll(0)


def producer_data_in_parallel(num_threads=3, batch_size=1000):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction, args=(i, batch_size))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except Exception as e:
        logger.error(f"Error in producing data: {e}")


if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    producer_data_in_parallel(num_threads=5, batch_size=1000)
