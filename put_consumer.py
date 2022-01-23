from kafka import KafkaConsumer

put_topic = "Put_Topic"
consumer = KafkaConsumer(put_topic)
for msg in consumer:
    message = msg.value
    print(message)


class PutConsumer:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(topic)

    def Start_consumer(self):
        for msg in self.consumer:
            print(msg.value)

PutConsumer("Put_Topic").start_consumer()
