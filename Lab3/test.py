def advanceConsumer():
    '''
    multipic topics and auto commit offset
    '''
    consumer = KafkaConsumer('bamboo1', 'bamboo2',
                             bootstrap_servers=['10.1.200.63:9092'],
                             group_id = '8_consumer_group',
                             auto_commit_enable=True,
                             auto_commit_interval_ms=30 * 1000,
                             auto_offset_reset='smallest')

    # initialize iteration
    for message in consumer:
        print("TOPIC:%s Partition:%d offset%d key=%s value=%s" % ( \
            message.topic, message.partition,
            message.offset, message.key,
            message.value))
        consumer.task_done(message)

    consumer.commit()

    # Batch process interface
    while True:
        for m in consumer.fetch_messages():
            print("===Topic:%s Partition:%d offset%d key=%s value=%s" % ( \
                message.topic, message.partition,
                message.offset, message.key,
                message.value))
            consumer.task_done(m)