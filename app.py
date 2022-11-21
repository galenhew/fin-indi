import streamlit as st
from dashboard import (FinTweepy, Bigquery, clown_tweets,
                       aaii_sentiment, get_snp_df, snp_merge, plot_snp_multi_indi)
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

# clown chart
df_clown = clown_tweets(df_tweets)
# st.dataframe(df_clown)
st.header("Clown Sentiment")
fig = px.line(df_clown, x="date", y="compound", color='name')
st.plotly_chart(fig)


# snp and aaii charts
df_aaii = aaii_sentiment()
df_spy = get_snp_df()

plot_dict = {'spy': df_spy,
            'aaii': df_aaii}

snp_m = snp_merge(plot_dict)
fig = plot_snp_multi_indi(snp_m)

st.header("S&P and Indicators")
st.plotly_chart(fig)

