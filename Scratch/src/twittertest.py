#import requests
#from requests_oathlib import OAuth1

from twitter import *


oathkey = '121499823-mn2Obrcam2Tb6UU6hNPH9ThLga2WfeT6snFPpqrM'
oathsecret = '4Sw41HS6mIy1aD8wwLi2ZsU3Dl93hT3VWEhI2MPT7KiYx'

consumerkey = 'QEjEmUkr5npg9sepDwkMtUf2B'
consumersecret = '7k7Y9hThbvYXLj9pWBEVKhEzAQ9GG8p0utSSMQ1LxXUUSAzqBa'



t = Twitter(  auth=OAuth(      
                            oathkey, oathsecret, consumerkey, consumersecret   ) )

tweets = t.search.tweets(q="#pycon")

print tweets[0]



'''
url = 'https://api.twiter.com/1.1/account/verify_credentials.json'

myauth = OAuth1('QEjEmUkr5npg9sepDwkMtUf2B','7k7Y9hThbvYXLj9pWBEVKhEzAQ9GG8p0utSSMQ1LxXUUSAzqBa')

requests.get(url,auth=myauth)

r = requests.get('https://api.twitter.com/1.1/search/tweets.json?q=%40ntilmans&src=typd')
'''