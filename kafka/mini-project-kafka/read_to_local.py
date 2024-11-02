from config.config import load_config

from kafka_handler.consume_and_produce import KafkaConsumeAndProduce

if __name__ == '__main__':

    producer_config = load_config('KAFKA_PRODUCRDER')
    consumer_config = load_config('KAFKA_CONSUMER_EXTERNAL')
    topic_source = ['product_view']
    topic_destination = 'kafka_project'

    handler = KafkaConsumeAndProduce(
        consumer_config,
        producer_config,
        topic_source,
        topic_destination
    )
    handler.consume()
