from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:29092",
    client_id="urbanpulse"
)

topic = NewTopic(
    name="city_events",
    num_partitions=1,
    replication_factor=1
)

try:
    admin.create_topics([topic])
    print("Topic created")
except Exception as e:
    print(e)

admin.close()

