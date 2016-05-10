

# python lang_scenario.py --couchdb_ip='115.146.95.99:5984' --tweets_dbname='yasmeen-test-tweets' --aurin_dbname='urban-data'


from couchdb import Server

import argparse
parser = argparse.ArgumentParser(description='')
parser.add_argument('--couchdb_ip', '-ip', help='ip of couchdb')
parser.add_argument('--tweets_dbname', '-db_tw', help='tweets db name')
parser.add_argument('--aurin_dbname', '-db_au', help='urban db name')
args = parser.parse_args()

server = Server('http://'+args.couchdb_ip+'/')

db = server[args.tweets_dbname]
db1 = server[args.aurin_dbname]




vic_lang = db1.view('scenarios/vic-health-suburb-top-languages')
tweets_place_lang = db.view('scenarios/search-lang-by-place',group_level=2)
index=0
stats_perc = []
stats_count = []
stats_total = []
langs_list = []
suburbs_list = []



for suburb in vic_lang:
    top_lang= suburb.value
    sub_place = suburb.key
    suburbs_list.append(sub_place)

    stats_perc.append([])
    stats_count.append([])
    stats_total.append([])
#    print sub_place
    top_lang.append("en")
    langs_list.append(top_lang)

    for lng in top_lang:
	total = 0
	sub_count=0
        for doc in  tweets_place_lang:
            tw_place =  doc.key[0]
            tw_lang = doc.key[1]
    	    if ( sub_place == tw_place):
	        count=doc.value
	        total+=count
	        if (tw_lang.lower() == lng):
	            sub_count=count

	if (total == 0 or sub_count ==0):
	    stats_perc[index].append(0)
	else:
	    stats_perc[index].append((float(sub_count)/float(total)))
        stats_count[index].append(sub_count)
    stats_total[index]=total


#	print 'index No '+ str(index) ,': ',sub_place ,lng , sub_count,' out of ',total
    index = index+1
#    break


vic_stats_perc = []
vic_stats_perc_view = db1.view('scenarios/vic-health-suburb-top-languages-perc')
index =0
for doc in vic_stats_perc_view:
    vic_stats_perc.append(doc.value)
    index =  index + 1


#print '############suburbs list##################'
#print suburbs_list
#print '\n\n'


#print '############top language list##################'
#print langs_list
#print '\n\n'


#print '############tweets count per language##################'
#print stats_count
#print '\n\n'

#print '############total no. of tweets per language##################'
#print stats_total
#print '\n\n'

#print '############percentage of tweets per language##################'
#print stats_perc
#print '\n\n'

#print '############vic health language percentage  per language##################'
#print vic_stats_perc
#print '\n\n'



import json
with open('lang-scen1.json', 'w') as f:
    json.dump({'VicHealth-langPerc' : vic_stats_perc,'Tweets-langPerc':stats_perc,'TweetsCount': stats_count,'NumofTweetPerSuburb':stats_total,'TopFiveLangList': langs_list,'SuburbsList':suburbs_list},f, ensure_ascii=False)



print 'json File is generated Successfully!!!'




