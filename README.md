# fin-indi

live app: https://galenhew-fin-indi1.streamlit.app/

App objective: Show live indicators

## Design considerations
- streamlit for fast FE dev time
- Google Bigquery for free and fast DB
- twitter api (non academic) can only pull tweets for the past 7 days. Thus we need to regularly store new tweets in DB.
<img width="1660" alt="gcp" src="https://user-images.githubusercontent.com/41975935/203263243-a6e26154-0d14-41b7-939a-2a7bbebd591f.png">

## Data sources
- ffn library
- AAII
- tweepy twitter

## Issues and Improvements
- Dependencies 
  - streamlit requires lower protobuf version than bigquery
- Speed
  - streamlit reloads page on inputs. Add streamlit cache decorators.
  - load BE in backgorund by adding asyncio to functions
- Data
  - AAII data by excel only for free. unable to scrape, unless selenium?
  - quandl has api but outdated
  - scheduled twitter pulls with redis, in it's own compute engine. Then FE only needs to pull full tweets data.
- ML
  - Sentiment analysis by Vader for dev speed
  - Use Bert model for more accuracy    
  - Tweets should be classified on different topics, and only relevant finance subjects processed for sentiment
  
## Steps
- ```pip install -r requirements.txt```
- Config files
  - dev:
    - add .streamlit folder with secrets.toml
    - streamlit run dashboard.py
    - add .streamlit/ to .gitignore file
  - prod:
    - Add secrets.toml to streamlit secrets in streamlit.io
    - configure git in streamlit.io and push to git
 

