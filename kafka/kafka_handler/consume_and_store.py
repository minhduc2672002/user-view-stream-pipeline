import json
import time

from kafka_handler.consumer_interface import ConsumerInterface

class KafkaConsumeAndStore(ConsumerInterface):
    def __init__(self, config, topic, mongo_client, db_name, collection_name):
        super().__init__(config,topic)
        self.data_batch = []
        self.BATCH_SIZE = 100000
        self.mongo_collection = mongo_client[db_name][collection_name]
        self.start_time = time.time()


    def inser_batch(self):
        self.mongo_collection.insert_many(self.data_batch)
        self.data_batch.clear()

    def handler(self, msg):
        try:
            self.data_batch.append(json.loads(msg.value().decode('utf-8')))
            if (len(self.data_batch) >= self.BATCH_SIZE) or (time.time() - self.start_time >= 5) :
                print(f"len batch {len(self.data_batch)}")
                self.inser_batch()
                self.reset_time()
        except Exception as e:
            if self.data_batch:
                first_message = self.data_batch[0]
                print(f"Insert lỗi batch từ timestamp: {first_message.get('time_stamp', 'Unknown')}")
            self.data_batch.clear()


    def reset_time(self):
        self.start_time=time.time()
        print("time da reset")

    def finalize(self):
        if len(self.data_batch) > 0:
            self.inser_batch()
    