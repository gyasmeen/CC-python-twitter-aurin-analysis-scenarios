from couchdb import Server


# python income_scenario.py --couchdb_ip='115.146.95.99:5984' --tweets_dbname='yasmeen-test-tweets' --aurin_dbname='urban-data'

import argparse
parser = argparse.ArgumentParser(description='')
parser.add_argument('--couchdb_ip', '-ip', help='ip of couchdb')
parser.add_argument('--tweets_dbname', '-db_tw', help='tweets db name')
parser.add_argument('--aurin_dbname', '-db_au', help='urban db name')
args = parser.parse_args()

server = Server('http://'+args.couchdb_ip+'/')

db = server[args.tweets_dbname]
db1 = server[args.aurin_dbname]


median_income = db1.view('scenarios/aurin-data-suburb-median-income')

tweets_place_sent = db.view('scenarios/search-sentiment-by-place',group_level=1)
index=0
suburbs_list=[]
income_list=[]
MedianIncome =[]
AveragePosPerc=[]
NumofTweets=[]
MaxPosPerc=[]
MinPosPerc=[]


for suburb in median_income:
    m_income= suburb.value
    sub_place = suburb.key
    sub_count=0
    for doc in tweets_place_sent:
        tw_place =  doc.key[0]
    	if (sub_place == tw_place):
	    res=doc.value
	    # income_list.append({'MedianIncome' :m_income,'AveragePosPerc': float(res['sum'])/float (res['count']), 'NumofTweets':(res['count']), 'MaxPosPerc': res['max'],'MinPosPerc':res['min']})
	    suburbs_list.append(sub_place)

	    MedianIncome.append(m_income)
	    AveragePosPerc.append( float(res['sum'])/float (res['count']))
  	    NumofTweets.append(res['count'])
	    MaxPosPerc.append(res['max'])
	    MinPosPerc.append(res['min'])


	    break
    index = index+1



mydict ={'SuburbsList': suburbs_list, 'MedianIncome' :MedianIncome,'AveragePosPerc': AveragePosPerc, 'NumofTweets':NumofTweets, 'MaxPosPerc': MaxPosPerc,'MinPosPerc':MinPosPerc}
import json
with open('median-family-income-scen3.json', 'w') as f:
    json.dump(mydict,f, ensure_ascii=False)



print 'json File is generated Successfully!!!'

