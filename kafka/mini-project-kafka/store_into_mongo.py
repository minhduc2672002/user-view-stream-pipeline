
from config.config import load_config
from pymongo import MongoClient 

from kafka_handler.consume_and_store import KafkaConsumeAndStore

if __name__ == '__main__':

    consumer_config = load_config('KAFKA_CONSUMER')
    topic= ['kafka_project']

    mongodb_config = load_config('MONGODB')
    client = MongoClient(mongodb_config['mongo_uri'])
    db = client[mongodb_config['mongo_db']]
    collection = db['mongo_collection']    


    handler = KafkaConsumeAndStore(
            consumer_config,topic,
            client,
            mongodb_config['mongo_db'],
            mongodb_config['mongo_collection']
        )

    try:
        handler.consume()
    except:
        client.close()
