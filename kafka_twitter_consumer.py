import json

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from twitter_responder import twitter_response
TEST = True
TOPIC = "tweets"

def main():
    print("kafka consumer")

    print('instantiating kafka consumer')
    kafka_consumer = KafkaConsumer(bootstrap_servers=['0.0.0.0:9092'],
                                   key_deserializer=lambda m: m.decode('utf8'),
                                   value_deserializer=lambda m: json.loads(m.decode('utf8')),
                                   auto_offset_reset="earliest",
                                   group_id='1')

    kafka_consumer.subscribe(['tweets'])

    for message in kafka_consumer:
        try:
            print(message) # this should be a dict
            twitter_response(message) # the work to be done with this message
        # all exceptions MUST be handled or the msg won't get commited and will cause irregular behavior
        except Exception as e:
            print(e)
            tp = TopicPartition(message.topic, message.partition)
            offsets = {tp: OffsetAndMetadata(message.offset, None)}
            kafka_consumer.commit(offsets=offsets)
        else:
            tp = TopicPartition(message.topic, message.partition)
            offsets = {tp: OffsetAndMetadata(message.offset, None)}
            kafka_consumer.commit(offsets=offsets)
            print("No messages to process. Sleeping at {}".format(datetime.datetime.now()))

if __name__ == "__main__":
    main()