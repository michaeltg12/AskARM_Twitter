import os
import json
import tweepy
from kafka import KafkaProducer
from config import *

SEARCH = "#askarmdata"
TOPIC = "tweets"

def main():
    if os.path.isfile("lock"):
        os.remove("lock")
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    print('starting stream listener')
    twitter_stream = tweepy.Stream(auth, Listener())
    twitter_stream.filter(track=[SEARCH], stall_warnings=True)


# set up stream listener
class Listener(tweepy.StreamListener):

    def on_data(self, data):
        print('on data triggered')

        all_data = json.loads(data)
        kafka_producer = connect_kafka_producer()
        publish_message(kafka_producer, TOPIC, "tweet", data)
        if kafka_producer is not None:
            kafka_producer.close()

        return True

    def on_error(self, status):
        print(status)

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


if __name__ == "__main__":
    main()
