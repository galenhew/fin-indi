import streamlit as st
from dashboard import FinTweepy
from streamlit_autorefresh import st_autorefresh
from google.oauth2 import service_account
from google.cloud import bigquery



# tweet = FinTweepy(config)
# df_author = tweet.get_author_df()
# df_timeline = tweet.get_user_timeline('from:jimcramer')
# df_all_users_timeline = tweet.get_all_users_timeline()

st.title("Stocks")

config= st.secrets
tweet = FinTweepy(config)
df_author = tweet.get_author_df()
df_timeline = tweet.get_user_timeline('from:jimcramer')
df_all_users_timeline = tweet.get_all_users_timeline()

st.dataframe(df_all_users_timeline)

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["google-bigquery"]
)
client = bigquery.Client(credentials=credentials)

