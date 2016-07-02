import MySQLdb
import re
import unicodedata
from multiprocessing import Pool

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.api import API
from tweepy.streaming import StreamListener

ckey = 'sRFEtftXsvwHgL6bU4Yhn1enJ'
csecret = '2eOv1Ov8k1ZYNUHxTmk0yUT2FE0rcBxBqQxN37TzuCGVNEQe6t'
atoken = '3194025414-kaDk8jjgzmJz2epomaeECbQsbPov6WijzioV3Rw'
asecret = 'OxGiF7ZdPC4ljiaBnoobCAhfpIyBB7Bn1HkwuDVln6Ge5'

# alchemy_api_keys = ['99ffde4abbe13ff7d46577fb1b32b989dcaba79f', 'c4d7b3ef2a002a3067f342dcdcd375e529ccb6e5',
# '9c12219c575293afaf896ee23c40f077e71bb9bb', '14f4bb53b16e90d0c7f917aad0941e43516ee84f']

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = API(auth, wait_on_rate_limit=True)

cnxn1 = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="final_twitter_data", charset="utf8",
                        use_unicode=True)
cursor1 = cnxn1.cursor()
cnxn1.autocommit(True)

# cnxn = MySQLdb.connect(host="localhost", user="root", passwd="harshit@123", db="college_info", charset="utf8",
# use_unicode=True)
# cursor = cnxn.cursor()
# cnxn.autocommit(True)

query1 = "set names 'utf8mb4'"
query2 = "SET CHARACTER SET utf8mb4"

# making both databases accept utf8mb4 as the data format in their columns
cursor1.execute(query1)
cursor1.execute(query2)

# log = open("error_log_collecting_tweets.txt", 'a')
# cursor.execute(query1)
# cursor.execute(query2)

# various api links which would be called form requests library
# alchemy_url = "http://access.alchemyapi.com/calls/text/TextGetTextSentiment"
# google_lang_det = "https://www.googleapis.com/language/translate/v2/detect"
# translate_url = "https://www.googleapis.com/language/translate/v2"
# google_get_local_time = "https://maps.googleapis.com/maps/api/timezone/json"
file_details = open('geo_enabled_no_location_tweets_1.txt', 'a+')

bb_coords_college = [-97.7369970802915, 30.2843508197085, -97.73429911970848, 30.2870487802915, -118.4465300802915,
                     34.06757201970849, -118.4438321197085, 34.07026998029149, -83.73957308029149, 42.27669461970849,
                     -83.73687511970849, 42.27939258029149, -78.529248, 38.0222905, -78.49484249999999, 38.0565153,
                     -82.3776244, 29.6136308, -82.3296963, 29.653098, -122.2719817, 37.8623341, -122.243466, 37.8766204,
                     -89.4459855, 43.0663376, -89.3941332, 43.0928874, -83.0485199, 39.9910582, -83.0071146, 40.0222629,
                     -77.88721, 40.782722, -77.84532, 40.815317, -83.38466249999999, 33.9226979, -83.3581749,
                     33.9619839, -96.38058029999999, 30.57385, -96.32373679999999, 30.6338273, -87.55417969999999, 33.2014806,
                     -87.52433789999999, 33.2225227, -76.50128289999999, 38.976712, -76.4729241, 38.994111, -84.40879319999999,
                     33.7684944, -84.38575890000001, 33.7890627, -88.246234, 40.083345, -88.20947989999999, 40.1164069,
                     -80.44775249999999, 37.1965848, -80.4043118, 37.2335951, -84.5052913, 42.6693478, -84.4618668, 42.7354626,
                     -93.2551422, 44.9655047, -93.21019439999999, 44.9916145, -76.70639198029151, 37.26338961970851,
                     -76.7036940197085, 37.2660875802915, -119.8815405, 34.4038983, -119.8394436, 34.4235783, -84.3065951,
                     30.4318294, -84.2830651, 30.4490398, -122.3219292, 47.6473905, -122.2876171, 47.663274, -79.9687639,
                     40.438087, -79.94772549999999, 40.4488853, -76.9617191, 38.9674974, -76.920996, 39.0071678,
                     -82.85645550000001, 34.6507891, -82.8177237, 34.6833028, -86.94598289999999, 40.4128266,
                     -86.9091327, 40.4684334, -81.0408721, 33.9823989, -81.0180049, 34.0076955, -85.5173092, 32.5765541,
                     -85.4804875, 32.6073784, -121.7685363, 38.5258507, -121.7414318, 38.5481164, -86.5366902, 39.1613616,
                     -86.4927871, 39.1942616, -94.18387299999999, 36.0565855, -94.1669727, 36.0740458, -97.0873103, 36.1196278,
                     -97.0626956, 36.1325264, -81.20140888029151, 28.6010784197085, -81.19871091970849, 28.6037763802915,
                     -91.588489, 41.6522478, -91.5297221, 41.6777466, -88.80363, 33.431284, -88.7765139, 33.47433, -78.7076175,
                     35.7522291, -78.6615003, 35.8037927, -95.2746602, 38.9427347, -95.2393975, 38.96751769999999, -97.454585,
                     35.1892195, -97.4310726, 35.2129452, -92.34657059999999, 38.9244146, -92.3122506, 38.9500382,
                     -93.66883179999999, 41.9967815, -93.6250439, 42.045452, -91.2002918, 30.3953726, -91.16610159999999,
                     30.4217624, -120.6638431802915, 35.3036563197085, -120.6611452197085, 35.3063542802915, -89.5516346,
                     34.3553851, -89.52636179999999, 34.3720666, -104.9089833, 38.9412437, -104.8012742, 39.0423526,
                     -72.31611219999999, 41.7758738, -72.2037942, 41.8546313, -96.7025913802915, 40.7882053197085,
                     -96.69989341970849, 40.7909032802915, -74.481439, 40.464441, -74.4132571, 40.5318925, -75.4600957,
                     38.6309905, -75.44968639999999, 38.6389647, -123.080209, 44.03891000000001, -123.061313, 44.0494364,
                     -78.8802339, 38.4187875, -78.8546355, 38.4481924, -110.9883719802915, 32.2473580197085, -110.9856740197085,
                     32.2500559802915, -82.426007, 28.0544963, -82.40178639999999, 28.0691412, -96.60125409999999,
                     39.18603299999999, -96.576098, 39.2064328, -66.1462246, 18.3671112, -66.14173459999999, 18.3739181,
                     -101.9112142, 33.5779995, -101.8706687, 33.6068696, -84.5150342, 38.0098018, -84.4953037, 38.044405,
                     -84.5238306, 39.1281267, -84.500652, 39.1404054, -117.2502095, 32.8701705, -117.2181877, 32.8918478,
                     -81.69438319999999, 36.2083561, -81.6736705, 36.2180581, -83.9476665, 35.9369125, -83.9212778, 35.9607901,
                     -111.9270538802915, 33.4093435197085, -111.9243559197085, 33.4120414802915, -117.0809228, 32.7701644,
                     -117.0621444, 32.7790934, -77.4578875, 37.5418817, -77.44611069999999, 37.5545334, -111.8533165,
                     40.7507678, -111.8167705, 40.7749279, -105.0960027, 40.5526073, -105.0720174, 40.5808081,
                     -86.8028281802915, 33.5020868197085, -86.80013021970849, 33.5047847802915, -85.7663164, 38.1959339,
                     -85.7517732, 38.2244429, -84.741621, 39.5003853, -84.7170364, 39.5233107, -118.1233843, 33.7750535,
                     -118.1079558, 33.7886864, -105.2672906802915, 40.0062320197085, -105.2645927197085, 40.00892998029149,
                     -117.859925, 33.6313019, -117.8218552, 33.6549079, -97.1637325, 33.19643, -97.1414301, 33.2175813,
                     -81.7987577, 32.4078083, -81.77234349999999, 32.4346482, -79.98743309999999, 39.6318055, -79.9493901,
                     39.6580404, -95.35252589999999, 29.7091819, -95.3331549, 29.7315486, -78.8172669802915, 42.9525137197085,
                     -78.8145690197085, 42.95521168029149, -105.235467, 39.7396608, -105.216788, 39.754302, -74.1780089,
                     40.7374229, -74.16778889999999, 40.7469695, -73.98544678029151, 40.7473993197085, -73.9827488197085,
                     40.75009728029149, -97.93969998029151, 29.8870620197085, -97.93700201970849, 29.8897599802915,
                     -75.15079338029149, 39.9800399197085, -75.14809541970848, 39.9827378802915, -80.38435009999999,
                     25.7510298, -80.3681946, 25.761216, -82.55569608029151, 35.4838070197085, -82.55299811970849,
                     35.4865049802915, -67.1489713, 18.2066856, -67.1355347, 18.2190486, -77.8280764, 42.7860158, -77.8175433,
                     42.8035248, -72.55003409999999, 42.3621071, -72.5103136, 42.4043704, -65.84035279999999, 18.1448545,
                     -65.83375649999999, 18.1492738, -75.8694274802915, 42.1046796197085, -75.8667295197085, 42.10737758029149,
                     -77.88262379999999, 34.2159196, -77.85594259999999, 34.2306444, -84.3907652, 33.7474135, -84.3799123,
                     33.7596849, -95.9045881, 45.5846263, -95.8894898, 45.5928139, -117.1722038, 46.7213542, -117.1284117,
                     46.7427935, -123.2968136, 44.5498621, -123.2672881, 44.5697689, -77.3774948, 35.5848461, -77.3601623,
                     35.6151051, -88.55461989999999, 47.108945, -88.5333271, 47.12186879999999, -91.23768659999999, 43.8127954,
                     -91.2227668, 43.818614, -75.9770649803, 40.3558975618, -75.9673718186, 40.3619229412, -122.1922819, 37.4151329,
                     -122.1502299, 37.4413934, -71.1089008, 42.3534404, -71.0800246, 42.3659977, -71.13617239999999,
                     42.3618469, -71.1068025, 42.3839481, -72.9370927, 41.2982032, -72.9188135, 41.3253851, -95.4139921,
                     29.7103203, -95.3937699, 29.7227147, -118.1293829, 34.1324301, -118.1212465, 34.1418231, -75.2092631,
                     39.9397406, -75.1819433, 39.959558, -74.6678009, 40.3318756, -74.6409745, 40.3516213, -90.31696,
                     38.6429082, -90.2999219, 38.6514981, -118.2914992, 34.0183008, -118.2800946, 34.0254396, -86.8199323,
                     36.1368835, -86.79283939999999, 36.151459, -91.65132, 41.91843391970851, -91.64756, 41.9211318802915,
                     -71.4114088, 41.8160857, -71.3879126, 41.83350859999999, -69.96611, 43.904022, -69.9525345, 43.9118338,
                     -73.9689102, 40.80348290000001, -73.95711899999999, 40.8124448, -86.25071419999999, 41.6908317,
                     -86.2167132, 41.7163003, -87.6062358, 41.7876879, -87.590174, 41.79504350000001, -117.7167032, 34.0939843,
                     -117.7070429, 34.1016205, -73.2155645, 42.705853, -73.19006680000001, 42.7229008, -77.0792111, 38.9050378,
                     -77.0689642, 38.91268230000001, -93.1592548, 44.4594767, -93.14107729999999, 44.4675558,
                     -76.49306729999999, 42.4320406, -76.4457346, 42.4818917, -72.2991516, 43.6969842, -72.2732492, 43.7237557,
                     -87.6947505, 42.0468328, -87.6692077, 42.0688091, -72.67098299999999, 41.54271079999999, -72.6501784,
                     41.5611646, -84.34565669999999, 33.787649, -84.30652549999999, 33.8057133, -72.5289327, 42.3662907,
                     -72.5103905, 42.3782996, -111.6506645802915, 40.2504945197085, -111.6479666197085, 40.2531924802915,
                     -73.1786775802915, 44.00695451970849, -73.17597961970849, 44.00965248029149, -79.45213009999999,
                     37.7848863, -79.44018609999999, 37.794382, -117.7114753, 34.0974713, -117.7024608, 34.1032263,
                     -76.6247027802915, 39.3235791197085, -76.62200481970848, 39.3262770802915, -71.30727668029151,
                     42.2922243197085, -71.3045787197085, 42.29492228029149, -80.29529169999999, 25.7115846, -80.2632769,
                     25.7316894, -92.723664, 41.7446876, -92.7146691, 41.7570002, -75.31369649999999, 40.0030933, -75.298779,
                     40.01262759999999, -76.8951608, 40.9473551, -76.87708980000001, 40.964431, -73.9652875802915,
                     40.8077515197085, -73.96258961970848, 40.8104494802915, -117.7131798, 34.1025491, -117.7070948,
                     34.1053825, -75.35766240000001, 39.8978264, -75.346053, 39.9096945, -73.9060806, 41.6576515, -73.8815331,
                     41.691854, -75.5487787, 42.8080363, -75.53169249999999, 42.8228189, -75.4129722, 43.0453223, -75.397761,
                     43.0594963, -80.8492232, 35.4964906, -80.8354384, 35.5066897, -72.6454519, 42.3101805, -72.6344695,
                     42.32212029999999, -71.176805, 42.3308314, -71.1562313, 42.3462259, -104.8306297, 38.8452077,
                     -104.8190663, 38.8523325, -77.54745659999999, 37.5683703, -77.534179, 37.5828858, -74.00038289999999,
                     40.7258318, -73.9932695, 40.732208, -71.1262394, 42.40053899999999, -71.11072589999999, 42.4114565,
                     -70.21072199999999, 44.1016779, -70.19413829999999, 44.1093923, -88.9951429802915, 40.5045471197085,
                     -88.9924450197085, 40.5072450802915, -93.1715789, 44.9342695, -93.1660272, 44.9412561, -79.9518791,
                     40.4402484, -79.9374246, 40.4490303, -118.3342177, 46.06793099999999, -118.3258438, 46.0732233,
                     -82.4011195, 40.3687905, -82.3902749, 40.382638, -73.93737, 42.814544, -73.9218467, 42.8214183,
                     -71.0954551, 42.3345314, -71.08333069999999, 42.3432364, -71.4102024, 41.825149, -71.40452130000001,
                     41.82904389999999, -80.2920581, 36.1140664, -80.2509017, 36.1420805, -88.1011905, 41.8657747,
                     -88.0919945, 41.8736062, -72.5777945, 42.2507279, -72.5647831, 42.2618789, -90.1238384, 29.9342754,
                     -90.1141595, 29.9481991, -76.3241054, 40.0431783, -76.31453739999999, 40.0548287, -82.21782188029151,
                     41.2909634197085, -82.21512391970849, 41.2936613802915, -121.9403364802915, 37.3482928197085,
                     -121.9376385197085, 37.3509907802915, -96.78712320000001, 32.8365549, -96.7737612, 32.84737870000001,
                     -71.2717166, 42.2918844, -71.259053, 42.3002364, -77.6378247, 43.1065431, -77.61067919999999, 43.1389289,
                     -75.3835713, 40.5987939, -75.3570709, 40.6110459, -97.3745554, 32.7019093, -97.3528924, 32.7137825,
                     -84.2056933, 39.7275676, -84.1626387, 39.74403090000001, -75.1674135, 44.5818448, -75.1545022, 44.5954109,
                     -122.6382337, 45.47885770000001, -122.6235887, 45.48473509999999, -75.3195511, 40.0239091, -75.3088064,
                     40.0316066, -71.124949, 42.3465092, -71.0931495, 42.3550949, -79.1887236, 37.3323922, -79.16230929999999,
                     37.3646204, -117.4282157, 47.7500332, -117.4084694, 47.758826, -87.3329475, 39.4788261, -87.3141333,
                     39.4868244, -71.26556479999999, 42.3589819, -71.2522005, 42.3712121, -118.2146071, 34.1243668,
                     -118.206396, 34.1303378, -93.19129099999999, 44.4559126, -93.17606669999999, 44.4690022, -122.4426922,
                     38.565888, -122.4382686, 38.5734565, -85.5854496802915, 42.9318262197085, -85.5827517197085,
                     42.9345241802915, -77.2433362, 39.8322909, -77.2311253, 39.83962520000001, -71.81293699999999,
                     42.2301637, -71.7999102, 42.242484, -76.1439146, 43.0324947, -76.1262593, 43.049482, -75.2149247,
                     40.69445640000001, -75.2022314, 40.7043239, -93.9798249802915, 44.32184141970851, -93.97712701970849,
                     44.3245393802915, -95.9515313, 36.1478079, -95.9403499, 36.1564606, -93.244225, 36.6129488,
                     -93.22937569999999, 36.6242493, -81.618764, 41.4978129, -81.5978501, 41.5158563, -84.78541919999999,
                     37.6410654, -84.7770845, 37.6486773, -75.348908, 40.0296581, -75.3340807, 40.0441007, -96.8561943,
                     32.4027356, -96.8493482, 32.4111914, -97.12501789999999, 31.5403688, -97.0987765, 31.5602849, -73.7904083,
                     43.0910103, -73.779122, 43.1009717, -117.7081172, 34.1024524, -117.7029731, 34.1068658, -95.9578136,
                     36.0463719, -95.9466381, 36.0554239, -92.4467377, 35.0953617, -92.4304637, 35.1029085]


class Listener(StreamListener):
    def __init__(self, api=None):
        # self.current_alchemy_api_key = random.choice(alchemy_api_keys)
        super(Listener, self).__init__()
        self.api = api or API()
        self.n = 1
        # self.count = 0

    def on_status(self, status):
        # limit = 0
        # trans_count = 0
        # tweet_decode = ""
        # global test_non_location

        try:
            # running loop for collecting tweets to infinity
            while self.n <= 2000000000:
                location_type = ''

                # print "inside while : ", self.current_alchemy_api_key
                # lon_w = -71.108403
                # lon_e = -71.084618
                # lat_s = 42.355553
                # lat_n = 42.364726
                # pprintpp.pprint(status)

                # saving the coordinates of the location of tweet

                if status.coordinates is not None:
                    # here we get the exact coordinates of the tweet

                    bb = (str(status.coordinates[u'coordinates']).replace('[', '').replace(']', '')).split(', ')
                    coord_lon = float(bb[0])
                    coord_lat = float(bb[1])
                    location_type = 'point'

                elif status.place is not None:
                    # here we are collecting the south-west and north-east coordinates of the bounding box as it encompasses
                    # the complete four side coordinates of the tweet location

                    bb = (str(status.place.bounding_box.coordinates[0][0] + status.place.bounding_box.coordinates[0][
                        2]).replace('[', '').replace(']', '')).split(', ')

                    # taking average of the bounding box coordinates to assume the approx. location coordinates
                    coord_lon = round((float(bb[0]) + float(bb[2])) / 2, 6)
                    coord_lat = round((float(bb[1]) + float(bb[3])) / 2, 6)
                    location_type = 'bb'

                else:
                    # test_non_location+=1
                    file_details.write("Geo Enabled Tweet doesn't have any sort of location, weird : \n" + status + "\n\n")
                    self.n += 1
                    return True

                # comparing the tweet coordinates with our pre-defined bounding box coordinates to check for authenticity (
                # given error range of (+-) 0.02)
                # if (lon_w - 0.02) <= coord_lon <= (lon_e + 0.02) and (lat_s - 0.02) <= coord_lat <= (lat_n + 0.02):
                #     locate = str(coord_lon) + ', ' + str(coord_lat)
                #     print locate
                # else:
                #     return True

                locate = str(coord_lon) + ', ' + str(coord_lat)
                # get_coords = str(coord_lat) + ', ' + str(coord_lon)
                mentions = ""
                hashtags = ""
                # translated_text = "NULL"
                mentions_list = status.entities[u'user_mentions']
                hashtags_list = status.entities[u'hashtags']

                # if 'media' in status.entities:
                #     for item in status.entities[u'media']:
                #         print '\n', item['type']
                #         if item['type'] == 'photo':
                #             link = item['media_url']
                #             print link

                # print mentions_list, hashtags_list

                if len(mentions_list) == 0:
                    mentions = "NULL"
                else:
                    for item in mentions_list:
                        mentions += str(item['screen_name'])
                        mentions += ', '

                if len(hashtags_list) == 0:
                    hashtags = "NULL"
                else:
                    for item in hashtags_list:
                        hashtags += str(item['text'])
                        hashtags += ', '

                followers_count = status.user.followers_count
                friends = status.user.friends_count
                num_status_post = status.user.statuses_count

                print '\n', self.n
                print unicodedata.normalize('NFKD', status.text).encode('utf8')

                # other required data to save in the database, tweets normalize to get the every character possible
                usr_name = unicodedata.normalize('NFKD', status.user.screen_name).encode('utf8')
                usr_twitter_id = status.user.id_str
                tweet_timestamp = str(status.created_at).split()
                tweet_decode = unicodedata.normalize('NFKD', status.text).encode('utf8')
                source = status.source

                # getting local date and time of the tweet for accurate mapping

                # gm_timestamp = time.mktime(time.strptime(str(status.created_at), "%Y-%m-%d %H:%M:%S"))
                # gm_timestamp = long(math.floor(gm_timestamp))
                # parame = {'key': 'AIzaSyCvpWn0CSxjr1CI4tJAIzwuKc3HftGZIA0', 'location': get_coords, 'timestamp': gm_timestamp}
                # get_local_time_info = requests.get(url=google_get_local_time, params=parame)
                # local_time = get_local_time_info.json()

                # if local_time['dstOffset'] is not None:
                # daylight_val = long(local_time['dstOffset'])
                # local_time_offset = long(local_time['rawOffset'])
                # tz_val = local_time['timeZoneId']

                # actual_timestamp = gm_timestamp + daylight_val + local_time_offset
                # actual_local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(actual_timestamp))
                # actual_local_time = str(actual_local_time).split()

                # print gm_timestamp, '\n', actual_local_time, '\n', local_time_offset, '\n', actual_timestamp, '\n', tz_val

                # toned down tweet text for sentiment analysing only the valid data
                tweet_api_pre = unicodedata.normalize('NFKD', status.text).encode('utf8', 'ignore').replace('@', ' ').replace('#', ' ') + " "
                tweet_api = tweet_api_pre

                while True:
                    get_data = re.findall(r'(http://t.co/[a-zA-Z0-9.]*[ ])', tweet_api)
                    if not get_data:
                        break
                    else:
                        for item in get_data:
                            tweet_api = re.sub(item, ' ', tweet_api)

                while True:
                    get_data_1 = re.findall(r'(https://t.co/[a-zA-Z0-9.]*[ ])', tweet_api)
                    if not get_data_1:
                        break
                    else:
                        for item1 in get_data_1:
                            tweet_api = re.sub(item1, ' ', tweet_api)

                # while True:
                #     get_data_2 = re.findall(r'(@[a-zA-Z0-9.]*[ ])', tweet_api)
                #     if not get_data_2:
                #         break
                #     else:
                #         for item in get_data_2:
                #             tweet_api = re.sub(item, ' ', tweet_api)

                # try:
                # Parameter list, to detect the tweet language from Google API
                # parms_det = {'key': 'AIzaSyCvpWn0CSxjr1CI4tJAIzwuKc3HftGZIA0', 'q': tweet_api}
                # lang_det = requests.get(url=google_lang_det, params=parms_det)
                # lang = lang_det.json()
                # temp = lang['data']['detections'][0][0]['language']
                # print temp

                # except Exception, e:
                # print "Error while calling tweet language detection on Google "
                # print "Error:", e
                # self.n += 1
                # return True
                temp = str(status.lang)

                # checking tweet language and converting into english
                # try:
                #
                #     if temp in ('en', 'und', 'english'):
                #         pass
                #         # Parameter list, containing the data to be sentimentally analysed
                #         # parameters = {'apikey': self.current_alchemy_api_key, 'text': tweet_api, 'outputMode': 'json',
                #         # 'showSourceText': 1}
                #
                #     else:
                #         # Parameter list, to translate the foreign language data into english
                #         # parms = {'key': 'AIzaSyAVGd_G9qFyEBEHR1_EctvczjFBIj7sd0U', 'q': tweet_api, 'target': 'en',
                #         #          'source': temp}
                #         #
                #         # data = requests.get(url=translate_url, params=parms)
                #         # answer = data.json()
                #         #
                #         # # get translated text for sentiment analysis
                #         # temp1 = answer['data']['translations'][0]['translatedText']
                #         # translated_text = temp1
                #         # print translated_text
                #         pass
                #
                #         # Parameter list, containing the data to be sentimentally analysed
                #         # parameters = {'apikey': self.current_alchemy_api_key, 'text': temp1, 'outputMode': 'json',
                #         #               'showSourceText': 1}
                #
                #         # getting sentiment analysed data from alchemy url
                #         # results = requests.get(url=alchemy_url, params=parameters)
                #         # response = results.json()
                #
                #         # try:
                #         #
                #         #     if 'OK' != response['status'] or 'docSentiment' not in response:
                #         #         print "Problem finding 'docSentiment' in HTTP response from AlchemyAPI"
                #         #         print response
                #         #         error_reason = response['statusInfo']
                #         #         print "HTTP Status:", results.status_code, results.reason
                #         #         print "--"
                #         #         self.n += 1
                #         #
                #         #         if error_reason == 'daily-transaction-limit-exceeded':
                #         #
                #         #             # current_alchemy_api_key being changed if the key's limit is reached
                #         #             if limit < 10:
                #         #                 reduced_alchemy_keys_list = [val for i, val in enumerate(alchemy_api_keys) if val !=
                #         #                                              self.current_alchemy_api_key]
                #         #                 self.current_alchemy_api_key = random.choice(reduced_alchemy_keys_list)
                #         #                 limit += 1
                #         #                 # continue to analyze for that tweet again with new sentiment key inside while loop
                #         #                 continue
                #         #             else:
                #         #                 return False
                #         #
                #         #         else:
                #         #             return True
                #         #
                #         #     sentiment_val = response['docSentiment']['type']
                #         #     score = 0.000
                #         #     if sentiment_val in ('positive', 'negative'):
                #         #         score = float(response['docSentiment']['score'])
                #         #
                #         # except Exception, e:
                #         #     print "D'oh! There was an error enriching Tweet "
                #         #     print "Error:", e
                #         #     print "Request:", results.url
                #         #     print "Response:", response
                #         #     self.n += 1
                #         #     return True
                #
                # except Exception, excpe:
                #     trans_count += 1
                #     print "Error in translation : ", excpe
                #     # self.n += 1
                #     # return True
                #     if trans_count < 10:
                #         continue
                #     else:
                #         print "Error with Google translation API please check"
                #         # time.sleep(2)
                #         return False

                # saving data into the MySQL database
                query = (
                    """insert into new_college_data (tweet,clean_tweet,user_name,user_twitter_id,source_tweet,"""
                    """lang_tweet,if_re,tweet_date,"""
                    """tweet_time,location,mentions,tags,people_follow,friends,status_count,location_type)"""
                    """values (%s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                )

                data = (tweet_decode, tweet_api, usr_name, usr_twitter_id, source, temp, str(status.retweeted),
                        tweet_timestamp[0], tweet_timestamp[1], locate, mentions, hashtags, followers_count, friends,
                        num_status_post, location_type)

                # cursor1.execute("set sql_notes = 0")
                cursor1.execute(query, data)
                # cursor1.execute("set sql_notes = 1")
                self.n += 1
                return True

        except Exception, excp:
            print 'Error in Status : ', self.n, str(excp) + "\n"
            # log.write(tweet_decode + str(excp))
            self.n += 1
            return True

    def on_error(self, status):
        print 'Error in your Syntax : ', self.n, str(status) + "\n"
        # self.counter = self.counter+1
        return False


# function to collect bounding box coordinates of every college according to Google Geocode API

# def get_coords():
#     query = "SELECT pub.college_name FROM pub_college_aspects_ranking pub LEFT JOIN pvt_college_aspects_ranking pvt " \
#             "ON pub.id = pvt.id UNION SELECT pvt.college_name FROM pub_college_aspects_ranking pub RIGHT JOIN
#               pvt_college_aspects_ranking pvt " \
#             "ON pub.id = pvt.id"
#     cursor.execute(query)
#     collect_data = cursor.fetchall()
#
#     for row in collect_data:
#         try:
#             input_val = str(row[0])
#             map_url = "https://maps.googleapis.com/maps/api/geocode/json"
#             # headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0'}
#             parameters = {'address': input_val, 'key': 'AIzaSyCvpWn0CSxjr1CI4tJAIzwuKc3HftGZIA0'}
#
#             bb_data = requests.get(url=map_url, params=parameters)
#
#             coordinates = bb_data.json()
#             bb = coordinates['results'][0]['geometry']['viewport']
#
#             lng_ne = bb['northeast']['lng']
#             lat_ne = bb['northeast']['lat']
#             lng_sw = bb['southwest']['lng']
#             lat_sw = bb['southwest']['lat']
#             bb_coords_college.extend([lng_sw, lat_sw, lng_ne, lat_ne])
#         except Exception as e:
#             print "Error in  ", row, " : coords_collection", e
#             continue
#
#     print bb_coords_college

def calc(coords_list_breakup):
    # print len(coords_list_breakup)
    try:
        twitterstream = Stream(auth, Listener())
        twitterstream.filter(locations=coords_list_breakup)
    except Exception, exc:
        print "Data access error : ", exc
        return True


# colleges bounding box collecting function is called first

# def init(l):
#     global lock
#     lock = l

# test_non_location = 0

if __name__ == "__main__":
    # get_coords()
    coords_college_divide = []
    prev = 0
    last = 0
    # test_non_location = 0

    try:
        while last < len(bb_coords_college):
            last = prev + 100
            # if prev == 0:
            #     add_list = bb_coords_college[-last:]
            # else:
            #     add_list = bb_coords_college[-last:-prev]
            add_list = bb_coords_college[prev:last]
            prev += 100
            coords_college_divide.append(add_list)

        # l = Lock()
        repeat_count = 0
        while True:
            # print "\n\nNew Batch of multiprocessing threads started\n\n"
            # current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            # file_details.write('\nNew Batch of multiprocessing threads started @ ' + current_time + '\n')
            repeat_count += 1
            if repeat_count < 50:
                pool = Pool(processes=5)
                return_data = pool.map(calc, coords_college_divide)
                # print "Process Ended : ", multiprocessing.current_process()
                # print "Non Location Tweets Misplaced : ", test_non_location
                file_details.write("\n\n************Returned Data to Processes***********\n\n")
                file_details.write("\n\nReturned data to a batch of processes : " + str(return_data) + "\n\n\n\n")
                # test_non_location = 0
                pool.close()
                pool.join()
            else:
                break

        cursor1.close()
        cnxn1.close()

    except Exception, e:
        print "Error while calling pool functions", e
