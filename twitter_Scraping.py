#Scrape tweets from twitter using snscrape 
import snscrape.modules.twitter as sntwitter
import pandas as pd
from pymongo import MongoClient
import streamlit as st
from datetime import datetime 

#Creating an UI using Streamlit
st.title("Twitter Scraping")
st.markdown("Using   'snscrape',  'MongoDB',  'streamlit'")

#@st.cache   
with st.container():
    
    #with st.form(key = 'form1'):

    name = st.text_input('Enter the keyword to be searched: ',key = 'name')

    tweet_count = st.number_input('No.of tweets to be scraped: ',min_value = 5,max_value = 100, step = 5, key = 'limit')

    start_date=st.date_input("Enter start date: ",  key = 'start_date')

    end_date = st.date_input("Enter end data:", key = "end_date")

    submit = st.button(label = 'Submit')

    #if submit:
    tweets_list = []
    # name = str(st.session_state["name"])
    # tweet_count = int(st.session_state["limit"])
    # start_date = str(st.session_state["start_date"])
    # end_date = str(st.session_state["end_date"])

    # Scrape twitter using snscrape module TwitterSearchScraper and append it to a list:
    for i,twt in enumerate(sntwitter.TwitterSearchScraper(f'from:{name} since:{start_date} until:{end_date}').get_items()): #declare a username 
        if i > tweet_count: #number of tweets you want to scrape
            break
        tweets_list.append([twt.date, twt.id, twt.content, twt.url, twt.replyCount, twt.retweetCount, twt.lang, twt.source, twt.likeCount, twt.user.username]) #declare the attributes to be returned
    
    # Creating a dataframe from the list: 
    tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'URL','ReplyCount','RetweetCount','Language','Source','LikeCount','Username'])
    #Displaying Scraped Tweets
    st.dataframe(tweets_df)

    st.download_button("Download as CSV", tweets_df.to_csv(), file_name='Tweets.csv',mime = 'text/csv')

    st.download_button("Download as JSON", tweets_df.to_json(), file_name='Tweets.json',mime = 'text/json')
            
  
    upload = st.button("Upload to DB")

    if upload:
        pyConnect = MongoClient("mongodb://kgeetha248:guvidw34@ac-3kffpy5-shard-00-00.utmmzha.mongodb.net:27017,ac-3kffpy5-shard-00-01.utmmzha.mongodb.net:27017,ac-3kffpy5-shard-00-02.utmmzha.mongodb.net:27017/?ssl=true&replicaSet=atlas-oz6uib-shard-0&authSource=admin&retryWrites=true&w=majority")
        pyDB=pyConnect["geetha"]    # database 
        pyCollection=pyDB["Twitter_Scraping"]   # Collection

        tweet_dict = {str(key):[data] for key, data in enumerate(tweets_list)}

        insert = pyCollection.insert_many([tweet_dict]) 

        st.success("Successfully Uploaded")

# column_1, column_2 = st.columns(2)
# with column_1:
#     #tweets_dict = st.session_state["tweets"]
#  st.download_button("Download as CSV", tweet_dict.to_csv(), file_name='Tweets.csv',mime = 'text/csv')

# with column_2:
#     #tweets_dict = st.session_state["tweets"]
#   st.download_button("Download as JSON", tweet_dict.to_json(), file_name='Tweets.json',mime = 'text/json')