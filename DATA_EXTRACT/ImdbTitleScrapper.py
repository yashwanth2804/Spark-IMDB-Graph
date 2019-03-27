import csv
import itertools
from lxml import html
import requests

#from cast1 import getCast
 

#Imdb top 250 -  https://www.imdb.com/chart/top
#Imdb top rated Indian - https://www.imdb.com/india/top-rated-indian-movies/
#Imdb top rated Telugu Movies - https://www.imdb.com/india/top-rated-telugu-movies
#Imdb top rated Tamil Movies - https://www.imdb.com/india/top-rated-tamil-movies/
#Imdb top rated Malayalam Movies - https://www.imdb.com/india/top-rated-malayalam-movies


pageUrl = 'https://www.imdb.com/chart/top'
page = requests.get(pageUrl)
tree = html.fromstring(page.content)

actors =[] 
moviesCast=[]
MoviesList = tree.xpath('//td[@class="titleColumn"]')
MoviesList = MoviesList[:2]

for i in MoviesList:
        title_url = i.xpath('./a/@href')
        str1 = ''.join(title_url)
        title = str1.split("/")[2] #prints tt0111161 -movie title 
        #movieCast = getCast(title)
        #actors.append(movieCast)
        #moviesCast.append(','.join(movieCast))



