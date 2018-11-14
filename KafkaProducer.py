from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
import json

producer = KafkaProducer(bootstrap_servers=['35.226.75.212:9094'])

# Asynchronous by default
future = producer.send('my-topic', b'raw_bytes_iot')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)

# produce keyed messages to enable hashed partitioning
producer.send('my-topic', key=b'foo', value=b'bar_iot')

# encode objects via msgpack
producer = KafkaProducer(bootstrap_servers=['35.226.75.212:9094'], value_serializer=msgpack.dumps)
producer.send('my-topic', {'key': 'value'})

# produce json messages
producer = KafkaProducer(bootstrap_servers=['35.226.75.212:9094'], value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer.send('json-topic', {'key': 'value'})

# produce asynchronously
for _ in range(1):
    producer.send('my-topic', 'msg_iot')

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# produce asynchronously with callbacks
producer.send('my-topic', 'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(bootstrap_servers=['35.226.75.212:9094'], retries=5)
