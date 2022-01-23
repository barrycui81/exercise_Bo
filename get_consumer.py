from kafka import KafkaConsumer

class GetConsumer:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(topic)


    def start_consumer(self):
        for msg in self.consumer:
            print(msg.value)

get_consumer = GetConsumer("Get_Topic")
get_consumer.start_consumer()
