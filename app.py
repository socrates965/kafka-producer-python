import os
import json
import time
import datetime;
from kafka import KafkaProducer

def produce_messages(delay=1):
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
    SSL_CHECK_HOSTNAME = os.getenv('SSL_CHECK_HOSTNAME')

    #environment variable dev
    # KAFKA_BOOTSTRAP_SERVER='kafka.apps.proddrc.customs.go.id:443'
    # KAFKA_USERNAME='kafka'
    # KAFKA_PASSWORD='kafka-secret'
    # KAFKA_TOPIC='test'
    # SECURITY_PROTOCOL='SASL_SSL'
    # SASL_MECHANISM='PLAIN'
    # SSL_CHECK_HOSTNAME=True
    DEBUG=True

    print("KAFKA_BOOTSTRAP_SERVER: ", KAFKA_BOOTSTRAP_SERVER)
    print("KAFKA_USERNAME: ", KAFKA_USERNAME)
    print("KAFKA_PASSWORD: ", KAFKA_PASSWORD)
    print("KAFKA_TOPIC: ", KAFKA_TOPIC)
    print("SECURITY_PROTOCOL: ", SECURITY_PROTOCOL)
    print("SASL_MECHANISM: ", SASL_MECHANISM)
    print("SSL_CHECK_HOSTNAME: ", SSL_CHECK_HOSTNAME)

    # create the producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             security_protocol=SECURITY_PROTOCOL,
                             sasl_mechanism=SASL_MECHANISM,
                             sasl_plain_username=KAFKA_USERNAME,
                             sasl_plain_password=KAFKA_PASSWORD,
                             ssl_check_hostname=SSL_CHECK_HOSTNAME,
                             max_block_ms=900000,
                             acks='all')

    # send messages
    x=0
    while True:
        time.sleep(delay)
        jsonpayload = json.dumps({'txt': f'hello {x}', 'datetime': datetime.datetime.now().isoformat()})
        print(f'sending {jsonpayload}')
        producer.send(KAFKA_TOPIC, jsonpayload.encode('utf-8'))
        x=x+1
    producer.flush()  # Important, especially if message size is small

produce_messages(2)