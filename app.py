import os
import json
import time
from kafka import KafkaProducer

#environment variable
KAFKA_BOOTSTRAP_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
KAFKA_USERNAME = os.environ.get('KAFKA_USERNAME')
KAFKA_PASSWORD = os.environ.get('KAFKA_PASSWORD')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')


def produce_messages(start=1, end=100, delay=1):
    """Sends a number of messages in JSON format '{"txt": "hello 1"}'

    Keyword arguments:
    start -- start number (default 0)
    end -- last number to send (default 100)
    delay -- number of seconds between messages (default 1)
    """

    # create the producer
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
                             security_protocol='SASL_SSL',
                             sasl_mechanism='PLAIN',
                             sasl_plain_username=KAFKA_USERNAME,
                             sasl_plain_password=KAFKA_PASSWORD,
                             api_version_auto_timeout_ms=30000,
                             max_block_ms=900000,
                             request_timeout_ms=450000,
                             acks='all')

    # send messages
    for x in range(start, end+1):
        time.sleep(delay)
        jsonpayload = json.dumps({'txt': f'hello {x}'})
        print(f'sending {jsonpayload}')
        producer.send(KAFKA_TOPIC, jsonpayload.encode('utf-8'))

    producer.flush()  # Important, especially if message size is small

produce_messages(1, 100, 2)