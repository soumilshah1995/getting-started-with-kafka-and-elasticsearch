try:
    from kafka import KafkaConsumer
    import json
    import requests
    import os
    import sys
except Exception as e:
    pass

# ------------------------------
os.environ['KAFKA_TOPIC'] = "FirstTopic"

# -----------------------------

def main():
    print("Listening *****************")

    consumer = KafkaConsumer(os.getenv("KAFKA_TOPIC"))

    for msg in consumer:

        payload = json.loads(msg.value)
        payload["meta_data"]={
            "topic":msg.topic,
            "partition":msg.partition,
            "offset":msg.offset,
            "timestamp":msg.timestamp,
            "timestamp_type":msg.timestamp_type,
            "key":msg.key,
        }
        print(payload, end="\n")




main()