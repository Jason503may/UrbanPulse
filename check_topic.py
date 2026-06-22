# check_topic.py

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers="localhost:29092"
)

topics = consumer.topics()

print("Topics:", topics)

for topic in topics:
    partitions = consumer.partitions_for_topic(topic)
    print(topic, partitions)


