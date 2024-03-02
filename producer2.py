from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


p = Producer({
    'bootstrap.servers': 'localhost:9092',
    'on_delivery': delivery_report
})

for i in range(10):
    data = {'message': 'Sensor data {}'.format(i), 'timestamp': '2022-01-01T00:00:00Z'}
    p.produce('sensor_data', key=str(i), value=json.dumps(data))

p.flush()