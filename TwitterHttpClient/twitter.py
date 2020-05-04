
import socket
import sys
import requests
import requests_oauthlib
import json
import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

ACCESS_TOKEN = '1002190093476614144-UKiIT8ItxW5SlOaOMFXSIORRs2UU86'
ACCESS_SECRET = '52sHTHcrY7xGyGdkFBSgU3wHlmrbZkQnYEjognLWRvTbq'
CONSUMER_KEY = 'hzFNAzjV9GUi8r3ORdP0n9ajP'
CONSUMER_SECRET = 'rOMEcVW5zhcOA0yd8sQ3k3QhVnmrqE9GCkMl324lXew3ucmXGU'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN,
ACCESS_SECRET)

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket
    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads( data )
            print( message['text'].encode('utf-8') )
            self.client_socket.send( message['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['football']) # this is the topic we are interested in



if __name__ == "__main__":
    new_skt = socket.socket()         # initiate a socket object
    host = "localhost"     # local machine address
    port = 2185                 # specific port for your service.
    new_skt.bind((host, port))        # Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)                 #  waiting for client connection.
    c, addr = new_skt.accept()        # Establish connection with client. it returns first a socket object,c, and the address bound to the socket

    print("Received request from: " + str(addr))
    # and after accepting the connection, we can send the tweets through the socket
    send_tweets(c)
