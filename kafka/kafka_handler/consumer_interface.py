from abc import ABC, abstractmethod
from confluent_kafka import Consumer,KafkaError

class ConsumerInterface(ABC):
    def __init__(self,consumer_config,topic):
        self.consumer = Consumer(consumer_config)
        self.topic = topic
        self.consumer.subscribe(self.topic)

    def consume(self):
        try:
            while True:
                # Đọc tin nhắn từ Kafka
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                    elif msg.error():
                        print('Error: {}'.format(msg.error()))
                        break
                else:
                    self.handler(msg)
        
        except KeyboardInterrupt:
            print("Consumer Stop !!!")
        finally:
            self.finalize()
            self.consumer.close()
    
    @abstractmethod
    def handler(self,msg):
        pass

    
    @abstractmethod
    def finalize(self):
        pass