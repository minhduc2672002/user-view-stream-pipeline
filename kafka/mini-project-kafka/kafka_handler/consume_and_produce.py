from confluent_kafka import Producer


from kafka_handler.consumer_interface import ConsumerInterface

class KafkaConsumeAndProduce(ConsumerInterface):
    def __init__(self, consumer_config,producer_config,topic_source,topic_destination):
        super().__init__(consumer_config,topic_source)
        self.topic_destination = topic_destination
        self.producer = Producer(producer_config)
    
    def delivery_report(self,err, msg):
        """ Callback sau khi gửi dữ liệu lên Kafka """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def handler(self, msg):
        try:
            message = msg.value()
            self.producer.produce(self.topic_destination,key=None, value=message, callback=self.delivery_report)
            self.producer.poll(0)
        except Exception as e:
            print(f"Produce Message Error!!! {e}")
    
    def finalize(self):
        pass
