try:
    from kafka import KafkaProducer
    from faker import Faker
    import json
    from time import sleep
except Exception as e:
    pass

producer = KafkaProducer(bootstrap_servers='localhost:9092')
_instance = Faker()


for _ in range(1000):
    _data = {
        "first_name": _instance.first_name(), "city":_instance.city(), "phone_number":_instance.phone_number(),
        "state":_instance.state()
    }
    _payload = json.dumps(_data).encode("utf-8")
    response = producer.send('FirstTopic', _payload)
    print(response)

    # print("is_done", response.is_done)
    # print("failed", response.failed())
    # print("succeeded", response.succeeded())
    sleep(1)

