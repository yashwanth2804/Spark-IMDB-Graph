import csv
import itertools
from lxml import html
import requests
import os
from progress.bar import Bar

from ImdbCastScrapper import getCast
 

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
#[<Element td at 0x7f4a175decb0>, <Element td at 0x7f4a175ded08>, .....]

if not os.path.exists("../DATA"):
    os.makedirs("../DATA")


bar = Bar('Processing', max=250)

for i in MoviesList:
        title_url = i.xpath('./a/@href')
        #['/title/tt0111161/?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=e31d89dd-322d-4646-8962-327b42fe94b1&pf_rd_r=BMDRYPWSQ8BK0T38TWB2&pf_rd_s=center-1&pf_rd_t=15506&pf_rd_i=top&ref_=chttp_tt_1']
        str1 = ''.join(title_url)
        title = str1.split("/")[2] #prints tt0111161 -movie title 
        movieCast = getCast(title)
        actors.append(movieCast)
        moviesCast.append(','.join(movieCast))
        bar.next()

merged = list(itertools.chain(*actors))
uniqueActors = set(merged)




# Saving actos uniqe list
with open('../DATA/Actors.csv','wb+') as file:
    for line in uniqueActors:
        file.write(line)
        file.write('\n')

file.close()


# Saving MovieCasts uniqe list
with open('../DATA/MoviesCast.csv','wb+') as file1:
    for line in moviesCast:
        file1.write(line)
        file1.write('\n')

file1.close()

bar.finish()
