import streamlit as st
from dashboard import FinTweepy, Bigquery
from streamlit_autorefresh import st_autorefresh



# tweet = FinTweepy(config)
# df_author = tweet.get_author_df()
# df_timeline = tweet.get_user_timeline('from:jimcramer')
# df_all_users_timeline = tweet.get_all_users_timeline()

st.title("Stocks")

config= st.secrets
print(config)
st.write(config)
st.write(config['tweepy'])

tweet = FinTweepy(config['tweepy'])
df_author = tweet.get_author_df()
df_timeline = tweet.get_user_timeline('from:jimcramer')
df_all_users_timeline = tweet.get_all_users_timeline()
st.dataframe(df_all_users_timeline)

# # Create API client.
# credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["google-bigquery"]
# )
# client = bigquery.Client(credentials=credentials)

gbq = Bigquery(config['google-bigquery'])
gbq.push_to_gbq_new_table(df_all_users_timeline)
df_tweet_incre = gbq.get_increment()
gbq.push_to_gbq_base(df_tweet_incre)
tweets = gbq.get_gbq_timeline()

st.dataframe(tweets)

