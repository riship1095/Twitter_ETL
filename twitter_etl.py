import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import airflow

def extract_tweets():
    #access keys and tokens
    access_key = 'vxHC7qPyBPbOCDw9jKXi8mAM8'
    access_secret = '**************************************'
    consumer_key = '1454991475754799104-9iQdPVmU2PbFpHukgm9Kdh535GZoMH'
    consumer_secret = '******************************************'

    #Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating API object
    api = tweepy.API(auth)

    musk_tweets = api.user_timeline(screen_name = '@elonmusk',
                               count=200,
                               include_rts = False,
                               tweet_mode = 'extended')
    
    tweet_list = []
    for tweet in musk_tweets:
        text = tweet._json['full_text']

        refined_tweet = {"user":tweet.user.screen_name,
                         "text": text,
                         "favorite_count":tweet.favorite_count,
                         "retweet_count":tweet.retweet_count,
                         "created_at": tweet.created_at}

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('files/elonmusk_twitter_data.csv')

def load_to_s3():
    df = pd.read_csv('files/elonmusk_twitter_data.csv')
    df.to_csv("s3://test-bucket-0410-01/elonmusk_twitter_data.csv")
