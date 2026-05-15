from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
	'orders',
	bootstrap_servers='localhost:9092',
	value_deserializer=lambda x: json.loads(x.decode('utf-8')),
	auto_offset_reset='latest'
)

for message in consumer:
	order = message.value
	if order['amount'] > 0:
		print(order)
