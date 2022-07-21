import requests
from bs4 import BeautifulSoup
import re
from kafka import KafkaProducer
from json import dumps
import time

## HTTP GET Request
url = 'https://sports.news.naver.com/news?oid=311&aid=0001473371'
req = requests.get(url, verify=False)

html_doc = req.text

soup = BeautifulSoup(html_doc, 'html.parser')
res = soup.find('div', id='newsEndContents')

res = res.text.replace('\n', '')
res = res.replace('\t', '')

producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

producer.send('rawdata', value=res)
producer.flush()
print("saved")