import streamlit as st
from streamlit_autorefresh import st_autorefresh
from google.oauth2 import service_account
from google.cloud import bigquery



# tweet = FinTweepy(config)
# df_author = tweet.get_author_df()
# df_timeline = tweet.get_user_timeline('from:jimcramer')
# df_all_users_timeline = tweet.get_all_users_timeline()

st.title("Stocks")

config= st.secrets
print(config)
st.write(config)
st.write(config['tweepy'])
st.write(st.secrets['tweepy'])

# # Create API client.
# credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["google-bigquery"]
# )
# client = bigquery.Client(credentials=credentials)

