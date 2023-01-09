import os
import json
import time
from kafka import KafkaProducer

def produce_messages(start=1, end=100, delay=1):
    """Sends a number of messages in JSON format '{"txt": "hello 1"}'

    Keyword arguments:
    start -- start number (default 0)
    end -- last number to send (default 100)
    delay -- number of seconds between messages (default 1)
    """
    #environment variable production
    KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
    KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
    KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
    SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL')
    SASL_MECHANISM = os.getenv('SASL_MECHANISM')

    #environment variable dev
    # KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
    # KAFKA_USERNAME = 'admin'
    # KAFKA_PASSWORD = 'admin-secret'
    # KAFKA_TOPIC = 'orders'

    print("KAFKA_BOOTSTRAP_SERVER: ", KAFKA_BOOTSTRAP_SERVER)
    print("KAFKA_USERNAME: ", KAFKA_USERNAME)
    print("KAFKA_PASSWORD: ", KAFKA_PASSWORD)
    print("KAFKA_TOPIC: ", KAFKA_TOPIC)
    print("SECURITY_PROTOCOL: ", SECURITY_PROTOCOL)
    print("SASL_MECHANISM: ", SASL_MECHANISM)

    # create the producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             security_protocol=SECURITY_PROTOCOL,
                             sasl_mechanism=SASL_MECHANISM,
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