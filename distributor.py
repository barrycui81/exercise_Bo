from kafka import KafkaConsumer, KafkaProducer

class Distributor:
    def __init__(self, topic, host):
        self.consumer = KafkaConsumer(topic)
        self.producer = KafkaProducer(bootstrap_servers = host)


    def start_distribute(self, sub_topic):
        for msg in self.consumer:
            message = msg.value
            print(message)
            if "GET" in str(message):
                self.producer.send(sub_topic["GET"], message)
                self.producer.flush()
            if "PUT" in str(message):
                self.producer.send(sub_topic["PUT"], message)
                self.producer.flush()

sub_topic = {
    "GET" : "Get_Topic",
    "PUT" : "Put_Topic",
    "DELETE": "Delete_Topic",
    "POST" : "Post_Topic"
}

dist = Distributor("Test_Topic", 'localhost:9092')
dist.start_distribute(sub_topic)
    
