from kafka import KafkaProducer
import pandas as pd
import time
import json

producer = KafkaProducer(
	bootstrap_servers='localhost:9092',
	value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

df = pd.read_csv('./orders.csv')

for index, row in df.iterrows():
	message = row.to_dict()
	producer.send('orders', value=message)
	print(f"Sent: {message}")
	time.sleep(1)

producer.flush()
