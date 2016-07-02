import MySQLdb
import itertools
import math
import time
from multiprocessing import Pool
from warnings import filterwarnings

cnxn = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="college_info", charset="utf8", use_unicode=True)
cursor = cnxn.cursor()

query3 = "select college_name,bounding_box from bounding_box_details"
cursor.execute(query3)
bb_college = cursor.fetchall()

cnxn.close()
cursor.close()

cnxn1 = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="final_twitter_data", charset="utf8",
                        use_unicode=True)
cursor1 = cnxn1.cursor()
cnxn1.autocommit(True)

query1 = "set names 'utf8mb4'"
query2 = "SET CHARACTER SET utf8mb4"

# making both databases accept utf8mb4 as the data format in their columns
cursor1.execute(query1)
cursor1.execute(query2)

filterwarnings('ignore')

check_stepping_value = open("count_of_starting_value_id_tweets.txt", 'a')


def tweets_div(start_val, step):
    # global conv_val
    # total_input1 = 0
    # total_conv1 = 0
    count = 0
    actual_count = 0
    print "Starting_val : ", start_val, " Step : ", step
    check_stepping_value.write("Starting_val : " + str(start_val) + " Step : " + str(step) + "\n\n")
    try:
        query = """select id,location from new_college_data order by id limit %s,%s """
        cursor1.execute(query, [start_val, step])
        loc_data = cursor1.fetchall()

        for row in loc_data:

            tweet_coords1 = str(row[1]).split(',')
            # print tweet_coords1
            tweet_coords = [float(item) for item in tweet_coords1]

            try:
                for bb_row in bb_college:

                    bb_coords1 = str(bb_row[1]).split(',')
                    # print bb_coords1
                    bb_coords = [float(i) for i in bb_coords1]

                    if ((bb_coords[2] + 0.01) >= tweet_coords[0] >= (bb_coords[0] - 0.01) and (bb_coords[3] + 0.01) >=
                        tweet_coords[1] >= (bb_coords[1] - 0.01)):

                        bb_row_strip = bb_row[0].replace(" ", "").replace("-", "").replace("&", "").replace(".", "").replace(",", "")
                        bb_row_strip = bb_row_strip[:56]
                        table_name = str(MySQLdb.escape_string(bb_row_strip + "_tweets"))
                        count += 1

                        try:
                            # cursor1.execute("set sql_notes = 0")
                            query4 = """create table if not exists """ + table_name + """ like new_college_data"""
                            cursor1.execute(query4)
                        except Exception, e:
                            print "Error in creating new table : ", e, " table : ", bb_row[0], " ", table_name
                            query00 = "update new_college_data set error_check ='Yes' where id = %s"
                            cursor1.execute(query00, [row[0]])
                            break

                        copy_data = ()

                        try:

                            # query6 = "select tweet,user_name,user_twitter_id,source_tweet,sentiment,sentiment_score,lang_tweet," \
                            #          "translated_tweet,location,tweet_date,tweet_time,if_re,mentions,tags,people_follow,friends," \
                            #          "status_count,local_tweet_date,local_tweet_time,local_tweet_tz from new_college_tweets " \
                            #          "where id = " + row[0]
                            #
                            # cursor1.execute(query6)
                            # get_college_data = cursor1.fetchall()
                            #
                            # for row_specific in get_college_data:

                            cursor1.execute("""select * from new_college_data where id = """ + str(row[0]))
                            copy_data = cursor1.fetchone()
                            # print copy_data, " ", len(copy_data), " ", copy_data[1]

                            data = (copy_data[1], copy_data[2], copy_data[3], copy_data[4], copy_data[5], str(copy_data[6]),
                                    copy_data[7], copy_data[8], copy_data[9], str(copy_data[10]), copy_data[11],
                                    str(copy_data[12]),
                                    str(copy_data[13]), copy_data[14], str(copy_data[15]), str(copy_data[16]),
                                    str(copy_data[17]), str(copy_data[18]), str(copy_data[19]), str(copy_data[12]),
                                    str(copy_data[13]), copy_data[22])

                            query5 = ("""insert into """ + table_name + """ (tweet,clean_tweet,user_name,user_twitter_id,"""
                                                                        """source_tweet,sentiment,sentiment_score,lang_tweet,"""
                                                                        """translated_tweet,"""
                                                                        """location,location_type,tweet_date,tweet_time,if_re,mentions,"""
                                                                        """tags,people_follow,friends,status_count,
"""
                                                                        """local_tweet_date,local_tweet_time,local_tweet_tz)"""
                                                                        """ values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"""
                                                                        """,%s,%s,%s,%s,%s,%s,%s,%s)""")

                            cursor1.execute(query5, data)
                            actual_count += 1

                        except Exception, e:
                            print "Error while inserting data : ", e
                            query00 = "update new_college_data set error_check ='Yes' where id = %s"
                            cursor1.execute(query00, [row[0]])
                            print "\n", copy_data

                        break

                    else:
                        continue

            except Exception, e:
                print "Error in checking for locations within bounding box : ", e
                query00 = "update new_college_data set error_check ='Yes' where id = %s"
                cursor1.execute(query00, [row[0]])
                continue

        print count, " : ", actual_count
        # global total_converted
        # global total_questioned
        # total_converted += actual_count
        # total_questioned += prev
        # print_val(actual_count, prev, "n")
        # global conv_val
        # global input_val
        #
        # conv_val.append(count)
        # input_val.append(actual_count)
        # global conv_val
        # conv_val.append(actual_count)
        # total_input += 500
        # print conv_val
        # print "Converted Tweets : ", actual_count

    except Exception, e:
        print "Error with database : ", e, "\n"

    finally:
        # for j, val in enumerate(conv_val):
        #     total_conv1 += val
        #     total_input1 += 5000
        # print "Conversion Ratio -> ", total_conv1, " : ", total_input1, "\n"
        return actual_count


# def print_val(total_converted, total_questioned, perm):
#     total_converted1 = 0
#     total_questioned1 = 0
#     if perm == "y":
#         for item in range(len(save_data)):
#             total_converted1 += item
#         for item1 in range(len(input_data)):
#             total_questioned1 += item1
#         print "Conversion Ratio -> ", total_converted1, " : ", total_questioned1
#     else:
#         save_data.append(total_converted)
#         input_data.append(total_questioned)

def func_star(a_b):
    """Convert `f([1,2])` to `f(1,2)` call."""
    try:
        return tweets_div(*a_b)
    except Exception, e:
        print "Error while calling dividing tweets function", e


def calling_main(first, last, step):
    try:
        pool = Pool(processes=1)
        tweets_assigned_college = pool.map(func_star, itertools.izip(range(first, last, step), itertools.repeat(step)))
        pool.close()
        pool.join()
        return tweets_assigned_college

    except Exception, excp:
        print "Error in calling main collection : ", excp


if __name__ == "__main__":
    total_conv = 0
    total_input = 0
    # total_conv2 = 0
    # total_input2 = 0
    add_range = 0
    stepping_val = 0
    prev = 74150855 #67137000  # 51320596  # 50386395 #47198982 #39591964 #34483413 #32198493 #27372577 #27342577 #27096808 #26000000 #25400000 #24500000 #23600000 #23300000 #22700000 #21400000 #20800000 #20100000 #19500000 #18800000 #18400000 #17800000 # 17489800 #17289800 #16689800 #16389800 #15989800 #15389800 #14789800 #14089800 #13289800 #12789800 #12089800 #11789900 #5490000
    # last = prev + add_range
    # query0 = """select count(tweet),max(id) from new_college_data where id > (select id from (select id from new_college_data join (select count(tweet) c from new_college_data) cnt on cnt.c = """+prev+""") t )"""

    query0 = """select count(tweet),max(id) from new_college_data"""
    cursor1.execute(query0)
    data2 = cursor1.fetchall()

    for row2 in data2:
        # print row2
        total_assignment_tweets, total_sent_tweets = [], []
        diff = row2[0] - prev
        id_max, temp = row2[1], row2[1]
        # n = 1000
        # loop_count = 0
        # prev_high,temp1 = row2[0],row2[0]

        try:

            while True:

                while diff > 500:
                    print "Max id before and after", id_max, " : ", temp
                    # print "Prev high before and after ",prev_high, " : ", temp1

                    id_max = temp
                    # prev_high = temp1

                    # add_range = diff
                    if diff <= 10000:
                        stepping_val = diff
                        last = prev + stepping_val
                    elif 10000 < diff <= 100000:
                        stepping_val = int(math.floor(diff / 5))
                        last = prev + (stepping_val * 5)
                    else:
                        stepping_val = int(math.floor(diff / 10))
                        last = prev + (stepping_val * 10)

                    print "Prev count value : ", prev, " New count Value : ", last

                    check_stepping_value.write("Prev count value : " + str(prev) + " New count Value : " + str(last) + "\n\n")

                    actual_tweets_assigned = calling_main(prev, last, stepping_val)
                    # comment : only when multiple processes running for this script

                    # print "len of conv_val : ", len(tweets_assigned_college)
                    #
                    # for k, val1 in enumerate(tweets_assigned_college):
                    #     total_conv += val1
                    #     total_input += 5000
                    # print "Conversion Ratio -> ", total_conv, " : ", total_input, "\n"
                    total_assignment_tweets.append(actual_tweets_assigned)
                    sub_sent_tweets = [stepping_val for _ in range(len(actual_tweets_assigned))]
                    total_sent_tweets.append(sub_sent_tweets)
                    prev = last
                    # print "len of conv_val : ", len(total_assignment_tweets)
                    # for k, val1 in enumerate(total_assignment_tweets):
                    #     for item in val1:
                    #         total_conv2 += item
                    #         total_input2 += 5000
                    #     print "Conversion Ratio -> ", total_conv2, " : ", total_input2, "\n"
                    # prev = last
                    # last += add_range
                    print "Max id inside check ", id_max
                    cursor1.execute(query0 + " where id > " + str(id_max))
                    data1 = cursor1.fetchall()
                    for row1 in data1:
                        diff = row1[0]
                        print "Diff inside", diff
                        # temp1 = row1[0]
                        temp = row1[1]
                        # print conv_val
                        # print_val(0, 0, "y")
                        # for item in range(len(conv_val)):
                        #     total_converted += item
                        # for item1 in range(len(input_val)):
                        #     total_questioned += item1
                        # print "Conversion Ratio -> ", total_converted, " : ", total_questioned

                # print "len of conv_val : ", len(total_assignment_tweets)
                else:
                    # loop_count += 1
                    # if loop_count < n:
                    print "Checked upto : ", prev
                    check_stepping_value.write("Checked count until : " + str(prev) + "\n\n")
                    print "Going to sleep for 30 minutes, don't terminate the script!"
                    time.sleep(60 * 30)
                    print "Max id after sleep ", id_max
                    cursor1.execute(query0 + " where id > " + str(id_max))
                    data1 = cursor1.fetchall()
                    for row1 in data1:
                        diff = row1[0]
                        print "Diff in after sleep ", diff
                        # temp1 = row1[0]
                        temp = row1[1]

            print "last indexed values : ", prev, " : ", prev + diff
            print total_sent_tweets
            for k, val1 in enumerate(total_assignment_tweets):
                index_count = 0
                for item in val1:
                    total_conv += item
                    total_input += total_sent_tweets[k][index_count]
                    index_count += 1
                print "Conversion Ratio -> ", total_conv, " : ", total_input, "\n"

            cursor1.close()
            cnxn1.close()

        except Exception, e:
            print "Error while calling for values : ", e, " having last checked count until : ", prev
            cursor1.close()
            cnxn1.close()
