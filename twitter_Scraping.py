#Scrape tweets from twitter using snscrape 
import snscrape.modules.twitter as sntwitter
import pandas as pd
from pymongo import MongoClient
import json
import requests
import streamlit as st
from streamlit_lottie import st_lottie
from datetime import datetime 

#Creating an UI using Streamlit
st.title("Twitter Scraping")
st.markdown("Using   'snscrape',  'MongoDB',  'streamlit'")

url1 = requests.get("https://assets9.lottiefiles.com/packages/lf20_5mhyg2hz.json")
url2 = requests.get("https://assets8.lottiefiles.com/packages/lf20_X4UwkZ.json")

url1_json = dict()
url2_json = dict()

if url1.status_code == 200:
    url1_json = url1.json()
else:
    print("Error in the URL")

if url2.status_code == 200:
    url2_json = url2.json()
else:
    print("Error in the URL")

st_lottie(url1_json)

#@st.cache   
with st.container():
    
    #with st.form(key = 'form1'):

    name = st.text_input('Enter the keyword to be searched: ',key = 'name')

    tweet_count = st.number_input('No.of tweets to be scraped: ',min_value = 5,max_value = 100, step = 5, key = 'limit')

    start_date=st.date_input("Enter start date: ",  key = 'start_date')

    end_date = st.date_input("Enter end data:", key = "end_date")

    submit = st.button(label = 'Submit')

    if "submit_state" not in st.session_state:
        st.session_state.submit_state = False
                
    if submit or st.session_state.submit_state:
        st.session_state.submit_state = True
    
    #if submit:
        with st.spinner("Scraping Tweets from Twitter..!"):
            tweets_list = []
            # Scrape twitter using snscrape module TwitterSearchScraper and append it to a list:
            for i,twt in enumerate(sntwitter.TwitterSearchScraper(f'from:{name} since:{start_date} until:{end_date}').get_items()): #declare a username 
                if i > tweet_count: #number of tweets you want to scrape
                    break
                tweets_list.append([twt.date, twt.id, twt.content, twt.url, twt.replyCount, twt.retweetCount, twt.lang, twt.source, twt.likeCount, twt.user.username]) #declare the attributes to be returned
            
            # Creating a dataframe from the list: 
            tweets_df = pd.DataFrame(tweets_list, columns=['Datetime', 'Tweet Id', 'Text', 'URL','ReplyCount','RetweetCount','Language','Source','LikeCount','Username'])
            #Displaying Scraped Tweets
            st.dataframe(tweets_df)
            st.balloons()      

            
        st.download_button("Download as CSV", tweets_df.to_csv(), file_name='Tweets.csv',mime = 'text/csv')

        st.download_button("Download as JSON", tweets_df.to_json(), file_name='Tweets.json',mime = 'text/json')
            
  
        upload = st.button("Upload to DB")

        if upload:
            pyConnect = MongoClient("mongodb://kgeetha248:guvidw34@ac-3kffpy5-shard-00-00.utmmzha.mongodb.net:27017,ac-3kffpy5-shard-00-01.utmmzha.mongodb.net:27017,ac-3kffpy5-shard-00-02.utmmzha.mongodb.net:27017/?ssl=true&replicaSet=atlas-oz6uib-shard-0&authSource=admin&retryWrites=true&w=majority")
            pyDB=pyConnect["geetha"]    # database 
            pyCollection=pyDB["Twitter_Scraping"]   # Collection

            #tweet_dict = {str(key):[data] for key, data in enumerate(tweets_list)}
            tweet_dict=tweets_df.to_dict("records")

            insert = pyCollection.insert_many(tweet_dict) 

            st.success("Successfully Uploaded")
            st_lottie(url2_json)

