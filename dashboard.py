import configparser
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import numpy as np

from google.cloud import bigquery
from google.oauth2 import service_account

import json
import tweepy # OAuth2.0 Version
import requests


################################### seceret keys ###################################
# dev
config = configparser.ConfigParser()
config.read(".streamlit/secrets.toml")

# prod

####################################################################################



class FinTweepy:


    def __init__(self, config):
        self.bearer_token  = config['tweepy']['bearer_token']
        self.api_key = config['tweepy']['api_key']
        self.api_key_secret = config['tweepy']['api_key_secret']
        self.access_token = config['tweepy']['access_token']
        self.access_token_secret = config['tweepy']['access_token_secret']
        self.client = tweepy.Client(bearer_token=self.bearer_token,
                               consumer_key=self.api_key,
                               consumer_secret=self.api_key_secret,
                               access_token=self.access_token,
                               access_token_secret=self.access_token_secret,
                               return_type=requests.Response,
                               wait_on_rate_limit=True)
        self.usernames = ["jimcramer","fundstrat","DaveHcontrarian", "GoldmanSachs"]


    def get_author_df(self):
        client = tweepy.Client(bearer_token=self.bearer_token)

        users_group = client.get_users(
            usernames =self.usernames,
            user_fields = ["created_at", "description","public_metrics","verified",],
        )

        pd_list= []
        for user in users_group.data:
            pd_list.append(user.data)

        df_author = pd.json_normalize( pd_list, sep="_" )
        df_author['id'] = pd.to_numeric(df_author['id'])
        df_author.head()
        return df_author


    def get_user_timeline(self, query):

        # get max. 100 tweets. search_recent_tweets gives past 7 days only.
        tweets = self.client.search_recent_tweets(query=query,
                                             tweet_fields=['author_id', 'created_at'],
                                             max_results=100)

        # Save data as dictionary
        tweets_dict = tweets.json()

        # Extract "data" value from dictionary
        tweets_data = tweets_dict['data']

        # Transform to pandas Dataframe
        df = pd.json_normalize(tweets_data)
        return df


    def get_all_users_timeline(self):
        # get timeline for all usernames
        df_list = []
        for i in self.usernames:
            df = self.get_user_timeline('from:' + i)
            df_list.append(df)

        df = pd.concat(df_list)

        # change types for gbq
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['author_id'] = pd.to_numeric(df['author_id'])
        df['id'] = pd.to_numeric(df['id'])
        df['text'] = df['text'].astype(str)
        df['edit_history_tweet_ids'] = df['edit_history_tweet_ids'].astype(str)

        return df


tweet = FinTweepy(config)
df_author = tweet.get_author_df()
df_timeline = tweet.get_user_timeline('from:jimcramer')
df_all_users_timeline = tweet.get_all_users_timeline()



class Bigquery:

    def __init__(self, config):
        file_json= "./streamlit/google-bigquery-fin-viz.json"
        self.bigquery_key_json = json.loads(file_json, strict=False)
        self.credentials = service_account.Credentials.from_service_account_info(self.bigquery_key_json)
        self.client = bigquery.Client(credentials=self.credentials)
        self.destination_table = 'tweets.timeline'
        self.destination_table_new = 'tweets.timeline_new'
        self.project_id = 'fin-viz'


    def push_to_gbq_new_table(self, df):

        # push new reads to adjacent table for sql comparison
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                # Specify the type of columns whose type cannot be auto-detected
                bigquery.SchemaField("text", bigquery.enums.SqlTypeNames.STRING),
                # Indexes are written if included in the schema by name.
                bigquery.SchemaField("edit_history_tweet_ids", bigquery.enums.SqlTypeNames.STRING),
            ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default
            # https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            df, self.destination_table_new, job_config=job_config
        )
        job.result()


    def get_increment(self):
        # read from GCP
        # see incremental not in timeline

        sql = """
            SELECT * FROM `fin-viz.tweets.timeline_new` 
            where text not in(select text from `fin-viz.tweets.timeline`)
            """

        df_new = self.client.query(sql).to_dataframe()
        df_new.head()
        return df_new


    def push_to_gbq_base(self, df_new):
        # push new reads to adjacent table for sql comparison
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                # Specify the type of columns whose type cannot be auto-detected
                bigquery.SchemaField("edit_history_tweet_ids", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("text", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("author_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
            ],
        )

        job = self.client.load_table_from_dataframe(
            df_new, self.destination_table, job_config=job_config
        )
        job.result()


    def get_gbq_timeline(self):
        # read from GCP
        # see incremental not in timeline

        sql = """
        SELECT * FROM `fin-viz.tweets.timeline` 
        """

        df = self.client.query(sql).to_dataframe()
        return df


gbq = Bigquery(config)
gbq.push_to_gbq_new_table(df_all_users_timeline)
df_tweet_incre = gbq.get_increment()
gbq.push_to_gbq_base(df_tweet_incre)
tweets = gbq.get_gbq_timeline()
