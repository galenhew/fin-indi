import configparser
import toml
import pandas as pd
import numpy as np
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from google.cloud import bigquery
from google.oauth2 import service_account

import tweepy # OAuth2.0 Version
import requests
import ffn


################################### seceret keys ###################################
# dev
config = toml.load(".streamlit/secrets.toml")

####################################################################################


class FinTweepy:


    def __init__(self, config):
        self.bearer_token  = config['bearer_token']
        self.api_key = config['api_key']
        self.api_key_secret = config['api_key_secret']
        self.access_token = config['access_token']
        self.access_token_secret = config['access_token_secret']
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


tweet = FinTweepy(config['tweepy'])
df_author = tweet.get_author_df()
df_timeline = tweet.get_user_timeline('from:jimcramer')
df_all_users_timeline = tweet.get_all_users_timeline()



class Bigquery:

    def __init__(self, config):
        self.credentials = service_account.Credentials.from_service_account_info(config)
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


gbq = Bigquery(config['google-bigquery'])
gbq.push_to_gbq_new_table(df_all_users_timeline)
df_tweet_incre = gbq.get_increment()
gbq.push_to_gbq_base(df_tweet_incre)
df_tweets = gbq.get_gbq_timeline()


def clown_tweets(df):
    sid = SentimentIntensityAnalyzer()

    # add author_name to sentiment df
    df_sent = df.merge(df_author[['name', 'id']]\
                    .rename(columns={'id': 'author_id'}), how="left", on="author_id")

    df_sent['scores'] = df_sent['text'].apply(lambda tweet: sid.polarity_scores(tweet))
    df_sent['compound'] = df_sent['scores'].apply(lambda score_dict: score_dict['compound'])
    df_sent['date'] = df_sent['created_at'].dt.normalize()
    # strftime('%Y-%m-%d')
    df_sent = df_sent.sort_values(by='created_at')

    def plots(author_dict, df):
        # filter by author_id and group by dates first to get sentiment per day. else jumbles authors.
        df1 = df[df['author_id'] == author_dict['id']]
        df1 = df1[['date', 'author_id', 'compound']].groupby('date').mean()
        df1['name'] = author_dict['name']
        return df1

    # concat grouped by dates authors
    df_clown_list = []
    df_author_dict = df_author[['name', 'id']].to_dict('records')

    for i in df_author_dict:
        df_clown_list.append(plots(i, df_sent))

    df_clown = pd.concat(df_clown_list).reset_index()

    return df_clown


df_clown = clown_tweets(df_tweets)
fig = px.line(df_clown, x="date", y="compound", color='name')
fig.show()

# aaii sentiment


def aaii_sentiment():
    # clean excel sheet. can be replaced by api

    aaii_raw = pd.read_excel('data/' + 'sentiment.xls', engine="xlrd", skiprows=range(1849, 2043), header=None)
    aaii_raw = aaii_raw.iloc[1:, :]
    aaii_header = aaii_raw.iloc[:3, :]
    aaii_header = aaii_header.fillna('')
    aaii_data = aaii_raw.iloc[4:, :]
    aaii_header_list = aaii_header.apply(lambda x: ' '.join(x.astype(str)).strip()).to_list()
    aaii = pd.DataFrame(
        np.row_stack([aaii_data.columns, aaii_data.values]),
        columns=aaii_header_list
    )

    # clean dataframe
    aaii['Bullish'] = pd.to_numeric(aaii['Bullish']).fillna(aaii['Bullish'].mean())
    aaii['Neutral'] = pd.to_numeric(aaii['Neutral']).fillna(aaii['Neutral'].mean())
    aaii['Bearish'] = pd.to_numeric(aaii['Bearish']).fillna(aaii['Bearish'].mean())
    aaii['Reported Date'] = pd.to_datetime(aaii['Reported Date'],  errors='coerce')
    aaii['Date']= aaii['Reported Date'].dt.normalize()
    aaii = aaii.sort_values(by='Date')
    aaii = aaii[['Date','Bullish','Neutral','Bearish']]

    # create compound score
    aaii['comp']= aaii['Bullish'] + aaii['Neutral']*0 + aaii['Bearish']*-1

    def normalize(df, feature_name):
        result = df.copy()
        # for feature_name in df.columns:
        max_value = df[feature_name].max()
        min_value = df[feature_name].min()
        result[feature_name + 'norm'] = (df[feature_name] - min_value) / (max_value - min_value)
        return result

    def fourier(df, senti_col):
        # extract sentiment score as a discrete fourier transform
        price_fourier = np.fft.fft(np.asarray(df[senti_col].tolist()))  # convert sentiment to FFT with numpy
        fourier_df = pd.DataFrame({'fourier': price_fourier})  # add to a dataframe
        fourier_list = np.asarray(fourier_df['fourier'].tolist())  # extract fourier score as array

        for num_ in range(5, 30, 5):  # create fourier columns with scores 20 and 25
            # compound fourier to smoothen signal
            fourier_list_m10 = np.copy(fourier_list)
            fourier_list_m10[num_:-num_] = 0
            # transform back into time spectrum append each fourier to dataframe with name of fourier
            df['fourier ' + str(num_)] = np.fft.ifft(fourier_list_m10)
            # convert fourier to real
            df['fourier ' + str(num_)] = df['fourier ' + str(num_)].apply(lambda x: np.real(x))

        # # plotting sentiment score and fourier transformed scores with different compounds
        # df[['fourier 5', 'fourier 10', 'fourier 15', 'fourier 20', 'fourier 25']].plot(figsize=(10, 6))

        # after choose fourier number
        df = df.drop(['fourier 5', 'fourier 10', 'fourier 15', 'fourier 20'], axis=1)
        df = normalize(df, 'fourier 25')
        return df

    df_f = fourier(aaii, 'comp')
    return df_f


def get_snp_df():
    spy = ffn.get('spy', start='2007-11-01')
    spy = spy.reset_index()
    spy['Date'] = pd.to_datetime(spy['Date']).dt.normalize()
    return spy


def snp_merge(plot_dict):
    spy_m = plot_dict['spy'].merge(plot_dict['aaii'], how="inner", on="Date")
    return spy_m


def plot_snp_multi_indi(df_f):

    # get 2SD lines on sentiment chart
    fourier_25_mean = df_f['fourier 25norm'].mean()
    fourier_25_sd = df_f['fourier 25norm'].std()
    fourier_25_mean + fourier_25_sd * 2
    fourier_25_mean - fourier_25_sd * 2

    df = df_f.copy()

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        specs=[[{"type": "scatter"}],
               [{"type": "scatter"}]]
    )

    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["spy"],
            mode="lines",
            name="SPY"
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["fourier 25norm"],
            mode="lines",
            name="AAII sentiment"
        ),
        row=2, col=1
    )
    fig.add_hline(y=fourier_25_mean + fourier_25_sd * 2, line_dash="dot", row=2, col=1,
                  annotation_text="2 SD",
                  annotation_position="bottom right")
    fig.add_hline(y=fourier_25_mean + fourier_25_sd, line_dash="dot", row=2, col=1,
                  annotation_text="1 SD",
                  annotation_position="bottom right")

    fig.add_hline(y=fourier_25_mean - fourier_25_sd, line_dash="dot", row=2, col=1,
                  annotation_text="1 SD",
                  annotation_position="bottom right")

    fig.add_hline(y=fourier_25_mean - fourier_25_sd * 2, line_dash="dot", row=2, col=1,
                  annotation_text="2 SD",
                  annotation_position="bottom right")

    # fig = px.line(df_clown, x="date", y="compound", color='name')
    # fig.show()

    return fig


df_aaii = aaii_sentiment()
df_spy = get_snp_df()

plot_dict = {'spy': df_spy,
            'aaii': df_aaii}

snp_m = snp_merge(plot_dict)
fig = plot_snp_multi_indi(snp_m)
fig.show()