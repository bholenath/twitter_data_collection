# from multiprocessing import Pool
# import random
import math
import time
# import unicodedata
from warnings import filterwarnings
import sys
import MySQLdb
import requests

# import re

# ckey = 'ZuOG4EnYSaytbMJrjujkE4m0G'
# csecret = '5kHVA3iQKUJ2xEwWi3OfnDnXePgr5qUrEoY8PljYChoOLARp5O'
# atoken = '99918081-CzzDcnKJPO8MR2OIcpHDEHoY9ti7wGAggcME3ke4h'
# asecret = 'NhyIvJLTygJwpHL9r0Ic2Gu2sPDl44dDvYtK5xfxMXWLS'

# alchemy_api_keys = ['99ffde4abbe13ff7d46577fb1b32b989dcaba79f', 'c4d7b3ef2a002a3067f342dcdcd375e529ccb6e5',
#                     '9c12219c575293afaf896ee23c40f077e71bb9bb', '14f4bb53b16e90d0c7f917aad0941e43516ee84f']

cnxn1 = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="final_twitter_data", charset="utf8",
                        use_unicode=True)
# cnxn1.set_character_set('utf8mb4')
# cnxn1.character_set_name('utf8mb4')
cursor1 = cnxn1.cursor()
cnxn1.autocommit(True)

cnxn = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="college_info", charset="utf8",
                       use_unicode=True)
cursor = cnxn.cursor()
cnxn.autocommit(True)
# cursor = cnxn.cursor()
# cnxn.autocommit(True)

query00 = "select * from time_conversion_scale"

cursor.execute(query00)
time_check_data = cursor.fetchall()

cursor.close()
cnxn.close()

query1 = "set names 'utf8mb4'"
query2 = "SET CHARACTER SET utf8mb4"

# making database accept utf8mb4 as the data format in their columns
cursor1.execute(query1)
cursor1.execute(query2)

filterwarnings('ignore')

# various api links which would be called form requests library
alchemy_url = "http://gateway-a.watsonplatform.net/calls/text/TextGetTextSentiment"
# google_get_local_time = "https://maps.googleapis.com/maps/api/timezone/json"

# error_sentiment = open('error_in_getting_sentiment.txt', 'a')
convert_count = open('count_of_conversion_for_each_college.txt', 'a')


def tables_calculate(count):
    try:
        query0 = """select table_name from information_schema.tables where table_schema = 'final_twitter_data' and table_name like '%_tweets' limit {},{}""".format(
            count, 200)
        # print query0
        cursor1.execute(query0)
        table_data = cursor1.fetchall()
        return table_data
    except Exception, e:
        print "Error while getting college names ", e


def adding_sentiment_time():
    count = 0

    try:
        current_alchemy_api_key = '4c31dc7176e538e5f2ebc3d662e728270e30efa4'
        # final = previous + 10
        table_data = tables_calculate(count)

        while True:
            try:
                for row1 in table_data:
                    tweet_count = 0
                    actual_analyzed = 0

                    query = """select id,clean_tweet,lang_tweet,translated_tweet,location,tweet_date,tweet_time,sentiment,error_check from """ + row1[
                        0] + """ order by id"""
                    cursor1.execute(query)
                    conversion_data = cursor1.fetchall()
                    print "\n\nNew table started : -> ", row1[0], "\n\n"
                    count += 1

                    for row in conversion_data:
                        # limit = 0

                        while True:
                            if row[7] in ('None', 'NULL', '', ' ') and row[8] == 'No':
                                tweet_count += 1
                                sentiment_val = ""
                                score = 0.000
                                tz_val = ""

                                try:

                                    if row[2] in ('en', 'und', 'unknown', 'fr', 'de', 'it', 'pt-BR', 'pt-PT', 'es', 'ru'):

                                        # tweet_api_pre = str(unicodedata.normalize('NFKD', row[1]).encode('utf8', 'ignore').replace('@', ' ')
                                        #                     .replace('#', ' ').replace('\\', ' ')) + " "
                                        # tweet_api = tweet_api_pre
                                        #
                                        # while True:
                                        #     get_data = re.findall(r'(http://t.co/[a-zA-Z0-9.]*[ ])', tweet_api)
                                        #     if not get_data:
                                        #         break
                                        #     else:
                                        #         for item in get_data:
                                        #             tweet_api = re.sub(item, ' ', tweet_api)
                                        #
                                        # while True:
                                        #     get_data_1 = re.findall(r'(https://t.co/[a-zA-Z0-9.]*[ ])', tweet_api)
                                        #     if not get_data_1:
                                        #         break
                                        #     else:
                                        #         for item1 in get_data_1:
                                        #             tweet_api = re.sub(item1, ' ', tweet_api)

                                        parameters = {'apikey': current_alchemy_api_key, 'text': row[1].encode('ascii', 'ignore'), 'outputMode': 'json',
                                                      'showSourceText': 1}

                                    else:
                                        if row[3] not in ('NULL', None, 'None', '', ' '):
                                            parameters = {'apikey': current_alchemy_api_key, 'text': row[3].encode('ascii', 'ignore'),
                                                          'outputMode': 'json', 'showSourceText': 1}
                                        else:
                                            break

                                    results = requests.get(url=alchemy_url, params=parameters)
                                    response = results.json()

                                    try:

                                        if 'OK' != response['status'] or 'docSentiment' not in response:
                                            print "Problem finding 'docSentiment' in HTTP response from AlchemyAPI"
                                            print response
                                            error_reason = response['statusInfo']
                                            print "HTTP Status:", results.status_code, results.reason
                                            print "--"

                                            if error_reason == 'daily-transaction-limit-exceeded':

                                                print "Daily Limit Exceeded, try again tomorrow"
                                                print "Table current : ", row1[0], "row id : ", row[0], "table_number : ", count
                                                convert_count.write(
                                                    "Table current : " + str(row1[0]) + "row id : " + str(row[0]) + "table_number : " + str(
                                                        count) + "\n\n")
                                                # query33 = "update " + row1[0] + " set error_check ='No' where id = %s"
                                                # cursor1.execute(query33, [row[0]])
                                                print "Waiting for the Alchemy Quota to rebuild, don't terminate\n"
                                                print "Current time : ", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), "\n"
                                                time.sleep(60 * 60)
                                                break
                                                # sys.exit()

                                                # current_alchemy_api_key being changed if the key's limit is reached
                                                # if limit < 10:
                                                #     reduced_alchemy_keys_list = [val for i, val in enumerate(alchemy_api_keys) if val !=
                                                #                                  current_alchemy_api_key]
                                                #     current_alchemy_api_key = random.choice(reduced_alchemy_keys_list)
                                                #     limit += 1
                                                #     # continue to analyze for that tweet again with new sentiment key inside while loop
                                                #     continue
                                                # else:
                                                #     sys.exit()

                                            else:
                                                # error_sentiment.write("\nReason for error : " + error_reason + " Language : " + response['language'] + " Text : " + response['text'] + "\n")
                                                query33 = "update " + row1[0] + " set error_check ='Yes' where id = %s"
                                                cursor1.execute(query33, [row[0]])
                                                break

                                        sentiment_val = response['docSentiment']['type']
                                        if sentiment_val in ('positive', 'negative'):
                                            score = float(response['docSentiment']['score'])

                                    except Exception, e:
                                        print "D'oh! There was an error in analysis result of the sentiment"
                                        print "Error:", e
                                        print "Request:", results.url
                                        print "Response:", response
                                        query22 = "update " + row1[0] + " set error_check ='Yes' where id = %s"
                                        cursor1.execute(query22, [row[0]])
                                        break

                                except Exception, e:
                                    print "Error while normalizing or getting sentiment of tweet : ", e
                                    query22 = "update " + row1[0] + " set error_check ='Yes' where id = %s"
                                    cursor1.execute(query22, [row[0]])
                                    break

                                try:
                                    # daylight_val = long(0)
                                    #
                                    # tweet_timestamp = ' '.join((str(row[5]), str(row[6])))
                                    # coords = row[4]
                                    # list_coords = coords.split(',')
                                    # get_coords = ','.join((list_coords[1], list_coords[0]))
                                    #
                                    # gm_timestamp = time.mktime(time.strptime(tweet_timestamp, "%Y-%m-%d %H:%M:%S"))
                                    # gm_timestamp = long(math.floor(gm_timestamp))
                                    #
                                    # # using google timezone api to get local time
                                    #
                                    # parame = {'key': 'AIzaSyBviWo-gQNgQuyAV2xwCVx01_I-qtkjbeo', 'location': get_coords,
                                    #           'timestamp': gm_timestamp}
                                    # get_local_time_info = requests.get(url=google_get_local_time, params=parame)
                                    # local_time = get_local_time_info.json()
                                    #
                                    # if 'dstOffset' in local_time:
                                    #     if local_time['dstOffset'] is not None or long(local_time['dstOffset']) != 0:
                                    #         daylight_val = long(local_time['dstOffset'])
                                    #
                                    # local_time_offset = long(local_time['rawOffset'])
                                    # tz_val = local_time['timeZoneId']

                                    tweet_timestamp = ' '.join((str(row[5]), str(row[6])))
                                    gm_timestamp = time.mktime(time.strptime(tweet_timestamp, "%Y-%m-%d %H:%M:%S"))
                                    gm_timestamp = long(math.floor(gm_timestamp))

                                    coords = row[4]
                                    list_coords = coords.split(',')
                                    list_coords = [float(item) for item in list_coords]

                                    local_time_offset = long(0)
                                    dst_offset = long(0)

                                    for row00 in time_check_data:
                                        # print row00, "\n", list_coords
                                        check_range = row00[2].split(',')
                                        check_range = [float(item1) for item1 in check_range]

                                        if check_range[2] >= list_coords[0] >= check_range[0] and check_range[3] >= list_coords[1] >= check_range[1]:
                                            local_time_offset = long(row00[3])
                                            tz_val = row00[1]
                                            if time.daylight == 1:
                                                dst_offset = long(row00[4])

                                        else:
                                            continue
                                        break

                                    actual_timestamp = gm_timestamp + local_time_offset + dst_offset
                                    actual_local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(actual_timestamp))
                                    actual_local_time = str(actual_local_time).split()

                                except Exception, e:
                                    # count += 1
                                    print "Error in converting date to local : ", e
                                    query44 = "update " + row1[0] + " set error_check ='Yes' where id = %s"
                                    cursor1.execute(query44, [row[0]])
                                    break
                                    # if count >= 10:
                                    #     time.sleep(3600)
                                    #     count = 0
                                    #     break
                                    # else:
                                    #     break

                                try:
                                    query3 = """update """ + row1[
                                        0] + """ set sentiment = %s, sentiment_score = %s, local_tweet_date = %s,local_tweet_time = %s, local_tweet_tz = %s  where id = %s"""

                                    cursor1.execute(query3,
                                                    (sentiment_val, score, actual_local_time[0], actual_local_time[1], tz_val, row[0]))
                                    actual_analyzed += 1
                                    break

                                except Exception, e:
                                    print "Error in updating the database :", e
                                    print "Table current : ", row1[0], "row id : ", row[0], "table_number : ", count
                                    query11 = "update " + row1[0] + " set error_check ='Yes' where id = %s"
                                    cursor1.execute(query11, [row[0]])
                                    # cursor1.close()
                                    # cnxn1.close()
                                    sys.exit(0)

                            else:
                                break

                    print "Tweets total : ", tweet_count, " Actually analyzed tweets : ", actual_analyzed
                    convert_count.write(
                        'College Name : ' + str(row1[0]) + ' total tweets : ' + str(tweet_count) + ' analyzed tweets : ' + str(actual_analyzed) + "\n\n")

                # else:
                #     count = 0
                #     table_data = tables_calculate(count)

            except Exception, e:
                print "Error with outer loop or calling tables_calculate after finishing one turn of all colleges analyzed : ", e, "Table no. last processed : ", count
                # cursor1.close()
                # cnxn1.close()
                sys.exit(1)

    except Exception, e:
        print "Error while calling outer function tables_calculate ", e, "Table no. last processed : ", count
        # cursor1.close()
        # cnxn1.close()
        sys.exit(1)


if __name__ == "__main__":
    # pool = Pool(processes=4)
    # pool.map(adding_sentiment_time, range(250, 1550, 100))
    # pool.close()
    # pool.join()
    adding_sentiment_time()
