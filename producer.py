from kafka import KafkaConsumer, KafkaProducer

class Producer:
    
    def __init__(self, topic, host):
        self.Topic_Name = topic
        self.producer = KafkaProducer(bootstrap_servers=host)

    def produce(self, m_list):
        for m in m_list:
            self.producer.send(self.Topic_Name, m.encode('utf-8'))
            self.producer.flush()

REST_cmd = [
    "DELETE http://localhost:8092/sprint-rest/foos/9",
    "PUT http://www.abc.com/test/drink/1",
    "POST http://www.abc.com/new",
    "GET /spring-rest/test/1 HTTP/1/1"
]

p_inst = Producer("Test_Topic", 'localhost:9092')
p_inst.produce(REST_cmd)
