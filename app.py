import streamlit as st
from dashboard import FinTweepy, Bigquery, clown_tweets
import plotly.express as px
from streamlit_autorefresh import st_autorefresh


st.title("Stocks")

# secret keys
config= st.secrets

# get data from twitter
tweet = FinTweepy(config['tweepy'])
df_author = tweet.get_author_df()
df_timeline = tweet.get_user_timeline('from:jimcramer')
df_all_users_timeline = tweet.get_all_users_timeline()

# push and pull bigquery data
gbq = Bigquery(config['google-bigquery'])
gbq.push_to_gbq_new_table(df_all_users_timeline)
df_tweet_incre = gbq.get_increment()
gbq.push_to_gbq_base(df_tweet_incre)
df_tweets = gbq.get_gbq_timeline()

st.dataframe(df_tweets)

df_clown = clown_tweets(df_tweets)
fig = px.line(df_clown, x="date", y="compound", color='name')
st.plotly_chart(fig)