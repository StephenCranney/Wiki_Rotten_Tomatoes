#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  4 20:24:08 2021

@author: stephencranney
"""
import urllib
import datetime
from urllib.request import Request, urlopen
from datetime import datetime
import re
import os
import sys
import string
import pandas as pd
import copy
import time
import timeit
#import the Beautiful soup functions to parse the data returned from the website
from bs4 import BeautifulSoup
#pip install "rotten_tomatoes_scraper"
from rotten_tomatoes_scraper.rt_scraper import MovieScraper
from urllib.error import HTTPError
import os

os.getcwd()


#To get all movies
#https://en.wikipedia.org/wiki/Lists_of_films

#https://en.wikipedia.org/wiki/List_of_films:_A


##############################Iterature through lists

Titles=[]
dsite= "https://en.wikipedia.org/wiki/List_of_films:_P"
page = urlopen(dsite)
soup = BeautifulSoup(page)
links=soup.find_all('a')
for i in links:
    try:
        temptitle=i.get("title")
    except SyntaxError:
        pass
    Titles.append(temptitle)
    
Titles = [i for i in Titles if i]    
temp = Titles.index("Television film")
remove_list = Titles[:temp]

movielists= [
    "https://en.wikipedia.org/wiki/List_of_films:_numbers",
    "https://en.wikipedia.org/wiki/List_of_films:_B",
    "https://en.wikipedia.org/wiki/List_of_films:_C",
    "https://en.wikipedia.org/wiki/List_of_films:_D",
    "https://en.wikipedia.org/wiki/List_of_films:_E", 
    "https://en.wikipedia.org/wiki/List_of_films:_F",
    "https://en.wikipedia.org/wiki/List_of_films:_G",
    "https://en.wikipedia.org/wiki/List_of_films:_H",
    "https://en.wikipedia.org/wiki/List_of_films:_I",
    "https://en.wikipedia.org/wiki/List_of_films:_J%2DK",
    "https://en.wikipedia.org/wiki/List_of_films:_L",
    "https://en.wikipedia.org/wiki/List_of_films:_M", 
    "https://en.wikipedia.org/wiki/List_of_films:_N%2DO", 
    "https://en.wikipedia.org/wiki/List_of_films:_P", 
    "https://en.wikipedia.org/wiki/List_of_films:_Q%2DR", 
    "https://en.wikipedia.org/wiki/List_of_films:_S",
    "https://en.wikipedia.org/wiki/List_of_films:_T",
    "https://en.wikipedia.org/wiki/List_of_films:_U%2DW",
    "https://en.wikipedia.org/wiki/List_of_films:_X%2DZ"
    ]


movielists= [
    "https://en.wikipedia.org/wiki/List_of_films:_C"
    ]

for j in movielists: 
    page = urlopen(j)
    soup = BeautifulSoup(page)
    links=soup.find_all('a')
    for i in links:
        try:
            temptitle=i.get("title")
        except SyntaxError:
            pass
        Titles.append(temptitle)

Titles = [i for i in Titles if i]    

Titles = [i for i in Titles if i not in remove_list]

#Lowercase
Titles = [each_string.lower() for each_string in Titles]

#Remove all with ( and explain multiple cases unknown 
Titles = [s.replace("(film)", "") for s in Titles]
Titles = [s.replace(" (film)", "") for s in Titles]

Tempelements=[]
Titles2 = copy.deepcopy(Titles) 
pattern = re.compile(r"\((\d+ film)\)")
for item in Titles2:
     tempelement=pattern.findall(item)
     Tempelements.append(tempelement)
     
Tempelements = [[item.replace(' film', '') for item in lst] for lst in Tempelements]

Titles=[(re.sub(r"\((.*?)\)", "", x)) for x in Titles]

Titles = [s.replace(":", "") for s in Titles]
Titles = [s.replace("–", "") for s in Titles]
Titles = [s.replace("\'", "") for s in Titles]
Titles = [s.replace("%2D", "") for s in Titles]
Titles = [s.replace(",", "") for s in Titles]
Titles = [s.replace(".", "") for s in Titles]
Titles = [s.replace("!", "") for s in Titles]
Titles = [item.strip() for item in Titles]
Titles = [s.replace(" ", "_") for s in Titles]

RTURL=[]
#Now combine years with titles:
for i in range(len(Titles)):
    try:
        tempx= Titles[i]+"_"+Tempelements[i][0]
    except IndexError:
        tempx=Titles[i]
    RTURL.append(tempx)
    
RTURL=[ x for x in RTURL if "edit_section" not in x ]

#####RT scrape

RTURL2=['apollo_13']

RTDF = pd.DataFrame(columns = ['title', 'year_genre_length', 
                               'num_critic_reviews', 'num_audience_reviews', 
                               'critic_score', 'audience_score', 'rating'])

for j in RTURL: 
    try:
        page = urlopen('https://www.rottentomatoes.com/m/'+j)
        soup = BeautifulSoup(page)
        
        #Get title, genre, year, user rating, critic rating, number of user ratings and critic ratings. 
        title = soup.find("div", {"class": "thumbnail-scoreboard-wrap"}).h1.text
        #To split later
        year_genre_length= soup.find("div", {"class": "thumbnail-scoreboard-wrap"}).p.text
        
        num_critic_reviews = soup.find("a", {"class": "scoreboard__link scoreboard__link--tomatometer"}).text
        num_audience_reviews = soup.find("a", {"class": "scoreboard__link scoreboard__link--audience"}).text
        
        movie_scraper = MovieScraper(movie_url='https://www.rottentomatoes.com/m/'+j)
        
        movie_scraper.extract_metadata()
        critic_score=movie_scraper.metadata.get('Score_Rotten')
        rating=movie_scraper.metadata.get('Rating')
        audience_score=movie_scraper.metadata.get('Score_Audience')
        
        df_early=pd.DataFrame()
        df_early['title'] = [title]
        df_early['year_genre_length'] = [year_genre_length]
        df_early['num_critic_reviews'] = [num_critic_reviews]
        df_early['num_audience_reviews'] = [num_audience_reviews]
        df_early['critic_score'] = [critic_score]
        df_early['audience_score'] = [audience_score]
        df_early['rating'] = [rating]
        
        RTDF=RTDF.append(df_early)
    except (HTTPError, UnicodeEncodeError):
        pass
    
#Other exception: No weird special characters
   
RTDF.to_csv('C:\\Users\\rache\\OneDrive\\Desktop\\RTDF_P.csv')

#######################Data wrangling once scrape is done:

import pandas as pd

filestouse= ["RTDF_B", "RTDF_C", "RTDF_D", "RTDF_E", "RTDF_F", "RTDF_G", "RTDF_H", "RTDF_I", "RTDF_JK", "RTDF_L", "RTDF_M",
             "RTDF_NO", "RTDF_P", "RTDF_QR", "RTDF_S", "RTDF_T", "RTDF_UVW_1", "RTDF_UVW_2", "RTDF_XYZ_", "RTDF_XYZ2_"]

RTDF=pd.read_csv('/Users/stephencranney/Desktop/RTDF/RTDF_numbers.csv')

for i in filestouse:
    rtdf= pd.read_csv('/Users/stephencranney/Desktop/RTDF/'+i+'.csv')
    RTDF=RTDF.append(rtdf)
    
RTDF['total_score']= RTDF['critic_score'] + RTDF['audience_score']

RTDF_tomerge=RTDF['year_genre_length'].str.split(',', expand=True)
RTDF_tomerge.columns=['year', 'genre', 'length']

RTDF = pd.concat([RTDF, RTDF_tomerge], axis=1)

RTDF.to_csv('/Users/stephencranney/Desktop/RTDF_full.csv')

#Only take thos with both scores (filters out small, high outlier cases)
RTDF = RTDF[RTDF['total_score'].notna()]

#Only take past a certain year (filters out "golden age" bias)
RTDF=RTDF[RTDF["year"]!="Drama"]

RTDF["year"]=RTDF["year"].astype(float)
RTDF = RTDF[RTDF['year']>1980]

RTDF = RTDF[RTDF['total_score']>150]

#Remove documentaries, biased upwards. 
RTDF = RTDF[RTDF['genre']!= ' Documentary']
RTDF = RTDF[RTDF['genre']!= ' Documentary/Gay & lesbian']
RTDF = RTDF[RTDF['genre']!= ' Anime/Sci-fi']
RTDF = RTDF[RTDF['genre']!= ' Documentary/Music']

#Remove small outliers
RTDF = RTDF[RTDF['num_audience_reviews']!= 'Fewer than 50 Ratings']
RTDF = RTDF[RTDF['num_audience_reviews']!= 'Fewer than 50 Verified Ratings']

#RTDF = RTDF[RTDF['num_audience_reviews']== '250,000+ Ratings']


RTDF=RTDF[['title', 'total_score', 'genre', 'audience_score', 'critic_score', 'year', 'num_critic_reviews', 'num_audience_reviews']]

RTDF.to_csv('/Users/stephencranney/Desktop/RTDF_abridged.csv')





patternDel = "(Documentary)"
filter = RTDF['genre'].str.contains(patternDel)
RTDF2 = RTDF[~filter]







