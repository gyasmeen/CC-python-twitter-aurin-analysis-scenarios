from couchdb import Server


# python age_scenario.py --couchdb_ip='115.146.95.99:5984' --tweets_dbname='yasmeen-test-tweets' --aurin_dbname='urban-data'

import argparse
parser = argparse.ArgumentParser(description='')
parser.add_argument('--couchdb_ip', '-ip', help='ip of couchdb')
parser.add_argument('--tweets_dbname', '-db_tw', help='tweets db name')
parser.add_argument('--aurin_dbname', '-db_au', help='urban db name')
args = parser.parse_args()

server = Server('http://'+args.couchdb_ip+'/')

db = server[args.tweets_dbname]
db1 = server[args.aurin_dbname]

median_age = db1.view('scenarios/aurin-data-suburb-median-age')

tweets_place_sent = db.view('scenarios/search-sentiment-by-place',group_level=1)
index=0
suburbs_list=[]
MedianAge =[]
AveragePosPerc=[]
NumofTweets=[]
MaxPosPerc=[]
MinPosPerc=[]
for suburb in median_age:
    m_age= suburb.value
    sub_place = suburb.key
    sub_count=0
    for doc in tweets_place_sent:
        tw_place =  doc.key[0]
    	if (sub_place == tw_place):
	    res=doc.value
	    suburbs_list.append(sub_place)
	    MedianAge.append(m_age)
	    AveragePosPerc.append(float(res['sum'])/float (res['count']))
	    NumofTweets.append(res['count'])
	    MaxPosPerc.append(res['max'])
	    MinPosPerc.append(res['min'])
	    break
    index = index+1

mydict ={'SuburbsList': suburbs_list, 'MedianAge' :MedianAge,'AveragePosPerc': AveragePosPerc, 'NumofTweets':NumofTweets, 'MaxPosPerc': MaxPosPerc,'MinPosPerc':MinPosPerc}
import json
with open('median-age-scen2.json', 'w') as f:
    json.dump(mydict,f, ensure_ascii=False)

print 'json File is generated Successfully!!!'
