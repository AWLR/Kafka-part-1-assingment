from confluent_kafka import Consumer, KafkaError
import json

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

def print_revocation(consumer, partitions):
    print('Revocation:', partitions)

def consume(consumer):
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
        else:
            print('Received message: {}'.format(msg.value().decode('utf-8')))
            data = json.loads(msg.value().decode('utf-8'))
            print('Message data: {}'.format(data))


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'sensor_data_consumer',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['sensor_data'], on_assign=print_assignment, on_revoke=print_revocation)

try:
    consume(c)
except KeyboardInterrupt:
    pass

c.close()