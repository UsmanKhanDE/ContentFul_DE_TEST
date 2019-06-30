import yaml
import json
import sqlite3
import pandas as pd
import sys
import tweepy
import psycopg2
from sqlalchemy import create_engine



# Fetch tweets of the any twitter handle passed as a first argument, it takes no of tweets as a 2nd argument
def get_influencer_tweets(influencer_handle, tweet_count):


    #To access Twitter API, All the secret keys and tokens will extracted and configured through a config file for security and accessibility
    with open("Config.yml", 'r') as configfile:
        cfg = yaml.load(configfile)

    consumer_key = cfg['twitter_keys']['Twitter_consumer_key']
    consumer_secret = cfg['twitter_keys']['Twitter_consumer_secret']
    access_token = cfg['twitter_keys']['Twitter_access_token']
    access_token_secret = cfg['twitter_keys']['Twitter_access_token_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name=influencer_handle, count=tweet_count, include_rts=False)
    return tweets


#Extract data from dictionary object into Dataframe
def Tweets_toDataFrame(tweets):
    Tweet_DF = pd.DataFrame()


#    DataSet['HashTags'] = [tweet.entities.get('hashtags') for tweet in tweets]

#     for item in tweets:
#         hash = item.entities['hashtags']
#         if 'hashtags':

#             for ht in hash:
#                     ht = [ht['text']  for ht in hash]
#                     ht.insert(0, item.id)
#         print(ht)


    Tweet_DF['tweetID'] = [tweet.id for tweet in tweets]
    Tweet_DF['created_at'] = [tweet.created_at for tweet in tweets]
    Tweet_DF['description'] = [tweet.user.description for tweet in tweets]
    Tweet_DF['favourites_count'] = [tweet.user.favourites_count for tweet in tweets]
    Tweet_DF['followers_count'] = [tweet.user.followers_count for tweet in tweets]
    Tweet_DF['geo_enabled'] = [tweet.user.geo_enabled for tweet in tweets]
    Tweet_DF['lang'] = [tweet.lang for tweet in tweets]
    Tweet_DF['location'] = [tweet.user.location for tweet in tweets]
    Tweet_DF['url'] = [tweet.user.url for tweet in tweets]
    Tweet_DF['verified'] = [tweet.user.verified for tweet in tweets]
    Tweet_DF['friends_count'] = [tweet.user.friends_count for tweet in tweets]
    Tweet_DF['tweet_id_str'] = [tweet.id_str for tweet in tweets]
    Tweet_DF['in_reply_to_screen_name'] = [tweet.in_reply_to_screen_name for tweet in tweets]
    Tweet_DF['in_reply_to_user_id'] = [tweet.in_reply_to_user_id for tweet in tweets]
    Tweet_DF['in_reply_to_user_id_str'] = [tweet.in_reply_to_user_id_str for tweet in tweets]
    Tweet_DF['is_quote_status'] = [tweet.is_quote_status for tweet in tweets]
    Tweet_DF['retweet_count'] = [tweet.retweet_count for tweet in tweets]
    Tweet_DF['retweeted'] = [tweet.retweeted for tweet in tweets]
    Tweet_DF['source'] = [tweet.source for tweet in tweets]
    Tweet_DF['text'] = [tweet.text for tweet in tweets]
    Tweet_DF['retweet_count'] = [tweet.retweet_count for tweet in tweets]
    Tweet_DF['retweeted'] = [tweet.retweeted for tweet in tweets]
    Tweet_DF['source'] = [tweet.source for tweet in tweets]
    Tweet_DF['text'] = [tweet.text for tweet in tweets]
    Tweet_DF['tweetFavoriteCt'] = [tweet.favorite_count for tweet in tweets]
    Tweet_DF['userID'] = [tweet.user.id for tweet in tweets]
    Tweet_DF['userScreen'] = [tweet.user.screen_name for tweet in tweets]
    Tweet_DF['userName'] = [tweet.user.name for tweet in tweets]

    return Tweet_DF

#Insert data from Twitter API to the Stagging tables of Postgre Database. Stagging tables get truncated and data gets purged for each batch
def StaggingData(Tweet_DF):
    with open("Config.yml", 'r') as configfile:
        cfg = yaml.load(configfile)

    Hostname = cfg['postgres_credentials']['Hostname']
    Username = cfg['postgres_credentials']['Username']
    Password = cfg['postgres_credentials']['Password']
    DatabaseName = cfg['postgres_credentials']['DatabaseName']
    Port = cfg['postgres_credentials']['Port']

    engine = create_engine('postgresql://' + Username + ':' + Password + '@' + Hostname + ':' + Port + '/' + DatabaseName)
    Tweet_DF.to_sql('Stg_Twitter', engine, if_exists='replace')

#Tables and Schema creation for the desired tables in the Model
def SchemaCreation():
    with open("Config.yml", 'r') as configfile:
        cfg = yaml.load(configfile)

    Hostname = cfg['postgres_credentials']['Hostname']
    Username = cfg['postgres_credentials']['Username']
    Password = cfg['postgres_credentials']['Password']
    DatabaseName = cfg['postgres_credentials']['DatabaseName']
    Port = cfg['postgres_credentials']['Port']


    try:
        connection = psycopg2.connect(user=Username,
                                      password=Password,
                                      host=Hostname,
                                      port=Port,
                                      database=DatabaseName)
        cursor = connection.cursor()


        create_tweet_user = '''CREATE TABLE tweet_user
              (
              tweetID bigint PRIMARY KEY     NOT NULL,
              created_at timestamp without time zone,
              description text ,
              favourites_count bigint,
              followers_count bigint,
              geo_enabled boolean,
              lang text ,
              location text ,
              url text ,
              verified boolean,
              friends_count bigint
              ); '''

        create_base_tweets = '''CREATE TABLE base_tweets
              (
              created_at timestamp without time zone,
              tweetID bigint PRIMARY KEY     NOT NULL,
              in_reply_to_screen_name text,
              in_reply_to_user_id text,
              in_reply_to_user_id_str text,
              is_quote_status boolean,
              retweet_count bigint,
              retweeted boolean,
              source text,
              text text,
              tweetFavoriteCt bigint,
              userID bigint,
              userScreen text,
              userName text
              ); '''

        cursor.execute(create_base_tweets)
        cursor.execute(create_tweet_user)
        connection.commit()
        print("Tables created successfully in PostgreSQL ")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL tables", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


#Data Migration from Stagging tables to DataStores which are keeping data for the defined retention
def DataStoreInsertion():
    with open("Config.yml", 'r') as configfile:
        cfg = yaml.load(configfile)

    Hostname = cfg['postgres_credentials']['Hostname']
    Username = cfg['postgres_credentials']['Username']
    Password = cfg['postgres_credentials']['Password']
    DatabaseName = cfg['postgres_credentials']['DatabaseName']
    Port = cfg['postgres_credentials']['Port']


    try:
        connection = psycopg2.connect(user=Username,
                                      password=Password,
                                      host=Hostname,
                                      port=Port,
                                      database=DatabaseName)
        cursor = connection.cursor()

        tweet_user_insertion_query = """ INSERT INTO tweet_user SELECT "tweetID",created_at,description,favourites_count,followers_count,
				                         geo_enabled,lang,verified,friends_count FROM "Stg_Twitter";"""

        base_tweets_insertion_query = """   INSERT INTO base_tweets SELECT created_at,"tweetID",in_reply_to_screen_name,in_reply_to_user_id,in_reply_to_user_id_str,
				                           is_quote_status,retweet_count,retweeted,source,text,"tweetFavoriteCt","userID","userScreen",
				                           "userName" FROM "Stg_Twitter";"""

        cursor.execute(tweet_user_insertion_query)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into tweet_user table")


        cursor.execute(base_tweets_insertion_query)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into base_tweets table")

    except (Exception, psycopg2.Error) as error:
        if (connection):
            print("Failed to insert record into test_tweet table", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

#Function to execute an query passed as an argument
def QueryData(Query):
    with open("Config.yml", 'r') as configfile:
        cfg = yaml.load(configfile)

    Hostname = cfg['postgres_credentials']['Hostname']
    Username = cfg['postgres_credentials']['Username']
    Password = cfg['postgres_credentials']['Password']
    DatabaseName = cfg['postgres_credentials']['DatabaseName']
    Port = cfg['postgres_credentials']['Port']


    try:
        connection = psycopg2.connect(user=Username,
                                      password=Password,
                                      host=Hostname,
                                      port=Port,
                                      database=DatabaseName)
        cursor = connection.cursor()
        cursor.execute(Query)
        print("Output Against the Requested Query:")
        Query_Output = cursor.fetchall()

        for row in Query_Output:
                     print(row)


    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while Accessing Data through PostgreSQL tables", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")



if __name__ == '__main__':
    tweets = get_influencer_tweets("NeymarJrSite",3)
    tweet_df = Tweets_toDataFrame(tweets)
    StaggingData(tweet_df)
    SchemaCreation()
    DataStoreInsertion()

    Query = ''' select bt.username,max(tu.followers_count) as followers,max(tu.favourites_count) as faves,sum(bt.retweet_count)/Count(*) as RT_Avg 
                from "tweet_user" tu
                LEFT JOIN
                "base_tweets" bt
                on
                bt.tweetid = tu.tweetid
                group by bt.username; '''

    QueryData(Query)

