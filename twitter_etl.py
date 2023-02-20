import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import airflow

def extract_tweets(ti):
    #access keys and tokens
    access_key = 'vxHC7qPyBPbOCDw9jKXi8mAM8'
    access_secret = '*****************************'
    consumer_key = '1454991475754799104-9iQdPVmU2PbFpHukgm9Kdh535GZoMH'
    consumer_secret = '*****************************'

    #Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating API object
    api = tweepy.API(auth)

    musk_tweets = api.user_timeline(screen_name = '@elonmusk',
                               count=200,
                               include_rts = False,
                               tweet_mode = 'extended')
    
    ti.xcom_push(key='tweeter_extract',value=musk_tweets)
    
def transform_tweets(ti):
    tweet_list = []
    musk_tweets = ti.xcom_pull(key="tweeter_extract", task_ids='extract_tweets')
    
    for tweet in musk_tweets:
        text = tweet._json['full_text']

        refined_tweet = {"user":tweet.user.screen_name,
                         "text": text,
                         "favorite_count":tweet.favorite_count,
                         "retweet_count":tweet.retweet_count,
                         "created_at": tweet.created_at}

        tweet_list.append(refined_tweet)
      
    ti.xcom_push(key='tweeter_transform',value=tweet_list)
    
def load_tweets(ti):
    tweet_list = ti.xcom_pull(key='tweeter_transform',task_ids='transform_tweets')
    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://test-bucket-0410-01/elonmusk_twitter_data.csv")
