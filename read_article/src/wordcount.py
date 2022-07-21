from kafka import KafkaConsumer
from json import loads

from kafka import KafkaProducer
from json import dumps
import time

# topic, broker list
consumer = KafkaConsumer(
    'rawdata',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=1000
)

# consumer list를 가져온다
print('[begin] get consumer list')
dic = dict()
for message in consumer:
    # print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % (
    #     message.topic, message.partition, message.offset, message.key, message.value
    # ))
    words = message.value.split()
    for word in words:
        if word not in dic:
            dic[word] = 0
        dic[word]+=1
print('[end] get consumer list')
print('word counting...')

producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

sorted_dict = sorted(dic.items(), key = lambda item: item[1], reverse = True)
print(sorted_dict)

producer.send('wordcount', value=sorted_dict)
producer.flush()

