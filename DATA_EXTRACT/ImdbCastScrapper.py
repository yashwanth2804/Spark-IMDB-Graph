from lxml import html
import requests

# gets title from ImdbTitleScrapper.py 

def getCast(titleid):
	casts =[]
	page = requests.get('https://www.imdb.com/title/'+titleid+'/')
	tree = html.fromstring(page.content)
	Casts = tree.xpath('//table[@class="cast_list"]/tr/td[@class="primary_photo"]')	 # [<Element td at 0x7fee28ed97e0>, <Element td at 0x7fee28ed9838>,....]
	for i in Casts:
		name = ''.join(i.xpath('./a/img/@alt')) # Robert Downey Jr
		casts.append(name)					
		
	return casts;