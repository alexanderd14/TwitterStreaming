#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import socket
import sys
import requests
import requests_oauthlib
import json


# In[ ]:


ACCESS_TOKEN = '1255989078677508097-CKvAl8wHJgWBEXpcwxW3ugqsvgBAZx'
ACCESS_SECRET = '3O9hhCEdwjMtU13X88J3Nry71U9NPLw9vPbLfCYNqzsmn'
CONSUMER_KEY = 'kTquJ9mt9vO5ALhxWkUNWOnHK'
CONSUMER_SECRET = 'ydTbtVwo68SjvnlUN3bwICH9gqy3svm240UtiuMJzg2L0FqLhp'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN,
ACCESS_SECRET)


# In[ ]:


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-122.75,36.8,-121.75,37.8,-74,40,-73,41'),('track','covid')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


# In[ ]:


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


# In[ ]:


TCP_IP = "localhost"
TCP_PORT = 2243
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)


# In[ ]:





# In[ ]:




