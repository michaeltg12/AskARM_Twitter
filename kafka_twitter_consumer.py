import re
import time
import json
import tweepy
import datetime
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from config import *
from nltk_pos_tag import processContent
from solr_search import search_solr

TEST = True
TOPIC = "tweets"

def main():
    print('twitter master')
    if TEST:
        CONSUMER_KEY = TEST_CONSUMER_KEY
        CONSUMER_SECRET = TEST_CONSUMER_SECRET
        ACCESS_TOKEN = TEST_ACCESS_TOKEN
        ACCESS_TOKEN_SECRET = TEST_ACCESS_TOKEN_SECRET

    while True:

        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth)

        kafka_consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                                       consumer_timeout_ms=1000,
                                       key_deserializer=lambda m: m.decode("utf-8"),
                                       value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                                       auto_offset_reset="earliest",
                                       group_id=1)
        kafka_consumer.subscribe(TOPIC)

        for message in kafka_consumer:
            try:
                this_result = json.loads(message.value.decode("utf-8"))
                print(this_result)
                # collect all desired data fields
                if 'text' in this_result:
                    tweet = this_result["text"]
                    username = this_result["user"]["screen_name"]
                    user_id = this_result["user"]["id"]
                    id_str = this_result["user"]["id_str"]

                    link_base_pattern = "https://adc.arm.gov/discovery/#v/results/s"

                    pos_tags = processContent(tweet, filter=True)
                    print("pos_tags = ",pos_tags)
                    tagged_words = [t[0] for t in pos_tags]
                    solr_query = " OR ".join(tagged_words)
                    solr_result = search_solr(solr_query, select=0)
                    recommended_ds = [ds["datastream"] for ds in solr_result.raw_response['response']['docs']]
                    rec_ds_links = ["{}/fdpl::{}".format(link_base_pattern,d) for d in recommended_ds]

                    temp = []
                    link_attrs1 = [link_base_pattern]
                    for key in category_dict.keys():
                        found_key_cat = re.findall(key.lower(), (tweet).lower())
                        if found_key_cat:
                            for key in found_key_cat:
                                temp.append(category_dict[key])
                    for each in set(temp):
                        link_attrs1.append(each)
                    link1 = '/'.join(link_attrs1)
                    print('\nlink1 --> {}'.format(link1))

                    temp = []
                    link_attrs3 = [link1]
                    for key in location_dict.keys():
                        found_key_loc = re.findall(key.lower(), (tweet).lower())
                        if found_key_loc:
                            for key in found_key_loc:
                                temp.append(location_dict[key])
                    for each in set(temp):
                        link_attrs3.append(each)
                    link3 = '/'.join(link_attrs3)
                    print('link3 --> {}'.format(link3))

                    temp = []
                    link_attrs4 = [link3]
                    if year.search((tweet).lower()):
                        found_year = re.findall('(\d{4})', (tweet).lower())
                        if len(found_year) == 1:
                            temp.append('d::{}-01-01,{}-12-31'.format(found_year[0], found_year[0]))
                        if len(found_year) > 1:
                            temp.append('d::{}-01-01,{}-12-31'.format(found_year[0], found_year[1]))
                    elif range.search((tweet).lower()):
                        found_years = range.findall((tweet).lower())
                        if len(found_years) == 2:
                            temp.append('d::{},{}'.format(found_years[0], found_years[1]))
                    for each in set(temp):
                        link_attrs4.append(each)
                    link4 = "/".join(link_attrs4)
                    print('link4 --> {}'.format(link4))

                    try:
                        if username:
                            t = str(datetime.datetime.now())[:-10]
                            tweet_list = []

                            if link4 != link_base_pattern:
                                tweet_list.append('@{0}, ARM has your data gen search --> {1} : {2}'.format(username, link4, t))
                            if len(rec_ds_links) > 0:
                                for link in rec_ds_links:
                                    tweet_list.append("@{0}, ARM has your datastream search --> {1} : {2}".format(username, link, t))
                            if len(tweet_list) > 0:
                                for tweet_back in tweet_list:
                                    try:
                                        api.update_status(tweet_back, in_reply_to_status_id=id_str)
                                        print("tweet_back --> {}".format(tweet_back))
                                    except tweepy.error.TweepError as twe4:
                                        print(twe4)
                    except Exception as e:
                        print(e)
                        tp = TopicPartition(message.topic, message.partition)
                        offsets = {tp: OffsetAndMetadata(message.offset, None)}
                        kafka_consumer.commit(offsets=offsets)
            except Exception as e:
                print(e)
                tp = TopicPartition(message.topic, message.partition)
                offsets = {tp: OffsetAndMetadata(message.offset, None)}
                kafka_consumer.commit(offsets=offsets)

        kafka_consumer.close()
        print("No messages to process. Sleeping at {}".format(datetime.datetime.now()))
        time.sleep(5)

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

callback_pattern = re.compile("@\w+")
year = re.compile("\d{4}")
range = re.compile("(\d{4}-\d{2}-\d{2})")
fromrange = re.compile("from (\d{4}|\d{8}) to (\d{4}|\d{8})")

category_dict = {
    "radiometric": "fcat::radio",
    "atmospheric": "fcat::atmstate",
    "atmosphere": "fcat::atmstate",
    "cloud":  "fcat::cloud",
    "clouds":  "fcat::cloud",
    "aerosols": "fcat::aeros",
    "aerosol": "fcat::aeros",
    "surface": "fcat::sfcprop",
    "carbon": "fcat::carbon",
    "CO2": "fcat::carbon"
}

measurement_dict = {
    "wind": "fpmt::horizwind",
    "moisture": "fpmt::atmmoist/fpmt::soilmoist",
    "optical": "fpmt::aerosoptical",
    # "irradiance": "fpmt::irradswnbdirnorm/fpmt::irradswbbtotdn/fpmt::irradswnbtotdn/"
    #               "fpmt::irradlwbbdn/fpmt::irradswbbdiffdn/fpmt::irradlwbbup/fpmt::irradswbbtotup/"
    #               "fpmt::irradswbbdirdn/fpmt::irradswbbdirnorm/fpmt::irradswspdiffdn",
    "radar": "fpmt::radardoppler/fpmt::radarpolar/fpmt::radarreflect",
    "doppler": "fpmt::radardoppler",
    # "aerosol": "fpmt::aerosoptdepth/fpmt::aerosscatter/fpmt::aerosabsorb/fpmt::aerosbackscatter/"
    #            "fpmt::aerosextinct/fpmt::aerosconc",
    "temperature": "fpmt::atmtemp",
    "atmosphere": "fpmt::atmtemp/fpmt::atmpres/fpmt::atmturb",
    "turbulence": "fpmt::atmturb",
    "polarization": "fpmt::radarpolar",
    "pressure": "fpmt::atmpres",
    "reflectivity": "fpmt::radarreflect",
    # "longwave": "fpmt::irradlwbbdn/fpmt::irradlwbbup/fpmt::radlwsp/fpmt::btlwsp/fpmt::radlwnb",
    # "cloud": "fpmt::cldfraction/fpmt::cloudbase/fpmt::cldoptdepth/fpmt::ccn/fpmt::cloudtopheight",
    "condensation" : "fpmt::ccn",
    "water path" : "fpmt::liqwaterpath",
    "height" : "fpmt::cloudtopheight",
    "depth" : "fpmt::cldoptdepth",
    "base" : "fpmt::cloudbase",
    "fraction": "fpmt::cldfraction",
    "scatter": "fpmt::aerosscatter",
    # "shortwave": "fpmt::irradswbbdiffdn/fpmt::irradswbbtotup/fpmt::irradswbbdirdn/fpmt::irradswnbdirdn/"
    #              "fpmt::irradswbbdirnorm/fpmt::irradswspdiffdn",
    "absorption": "fpmt::aerosabsorb",
    "radiation": "fpmt::aerosbackscatter/fpmt::backscatter",
    "precipitation": "fpmt::precip",
    "water" : "fpmt::precipwater",
    "spectral": "fpmt::radlwsp",
    "radiance": "fpmt::radlwsp/fpmt::radlwnb",
    "soil temp": "fpmt::soiltemp",
    "soil temperature": "fpmt::soiltemp",
    "soil moisture": "fpmt::soilmoist",
    "cloud base height": "fpmt::cloudbase",
    "cloud height": "fpmt::cloudbase",
    "shortwave broadband direct downwelling irradiance": "fpmt::irradswbbdirdn",
    "longwave spectral brightness temperature": "fpmt::btlwsp",
    "soil surface temperature": "fpmt::soiltemp",
    "hygroscopic growth": "fpmt::hygrogrowth",
    "surface albedo": "fpmt::sfcalbedo",
    "planetary boundary layer height": "fpmt::pblheight",
    "vertical velocity": "fpmt::verticalvel",
    "cloud optical depth": "fpmt::cldoptdepth",
    "liquid water path": "fpmt::liqwaterpath",
    "aerosol extinction": "fpmt::aerosextinct",
    "precipitable water": "fpmt::precipwater",
    "longwave narrowband brightness temperature": "fpmt::btlwnb",
    "advective tendency": "fpmt::advecttend",
    "latent heat flux": "fpmt::latenthtflx",
    "shortwave narrowband direct downwelling irradiance": "fpmt::irradswnbdirdn",
    "sensible heat flux": "fpmt::senshtflx",
    "cloud condensation nuclei": "fpmt::ccn",
    "backscattered radiation": "fpmt::backscatter",
    "cloud top height": "fpmt::cloudtopheight",
    "longwave narrowband radiance": "fpmt::radlwnb",
    "shortwave broadband direct normal irradiance": "fpmt::irradswbbdirnorm",
    "shortwave spectral diffuse downwelling irradiance": "fpmt::irradswspdiffdn",
    "microwave narrowband brightness temperature": "fpmt::btmwnb",
    "aerosol concentration": "fpmt::aerosconc"
}

location_dict = {
    " sgp " : "fsite::sgp",
    "southern great plains" : "fsite::sgp",
    " nsa " : "fsite::nsa",
    "north slope of alaska" : "fsite::nsa",
    "north slope alaska" : "fsite::nsa",
    " twp " : "fsite::twp",
    "tropical western pacific" : "fsite::twp",
    " ena " : "fsite::ena",
    "eastern north atlantic" : "fsite::ena",
    " pvc " : "fsite::pvc",
    " mao " : "fsite::mao",
    " oli " : "fsite::oli",
    " asi " : "fiste::asi",
    " mar " : "fiste::mar",
    " mag " : "fiste::mag",
    " ship" : "fsite::mar",
    " grasslands " : "fsite::sgp",
    "mobile site" : ["fsite:pvc","fsite::mao","fsite::pye","fsite::pgh","fsite::hfe","fsite::grw","fsite::fkb",
                     "fsite::nim","fsite::asi","fsite::oli","fsite::awr","fsite::tmp","fsite::sbs","fsite::mag",
                     "fsite::acx","fsite::gan","fsite::mar"],
    "permanent site" : ["fsite::sgp","fsite::nsa","fsite::twp","fsite::ena"],
    " tundra " : ["fsite::nsa","fsite::oli"],
    "atlantic" : ["fsite::pvc","fsite::ena", "fsite::asi"],
    "pacific" : ["fsite::pye","fsite::mag","fsite::acx","fsite::mar","fsite::twp"],
    " china" : "fsite::hfe",
    "australia" : "fsite:twp",
    "asia" : ["fsite::hfe", "fsite::pgh", "fsite::gan"],
    "africa" : "fsite::nim",
    "europe" : "fsite::fkb",
    "south america" : ["fsite::mao","fsite::cor"],
    "north america" : ["fsite::nsa","fsite::oli","fsite::sgp","fsite::pye",
                       "fsite::mag","fsite::acx","fsite::sbs","fsite::pvc"]
}
data_products ={
    "mfrsr" : ["fdsc::mfrsraod1mich","fdsc::mfrsr"]
}

'''
Permanent Sites
Southern Great Plains sgp
North Slope Alaska nsa
Tropical Western Pacific twp
Eastern North Atlantic ena

Mobile Sites
Cape Cod, MA, USA; Mobile Facility (TCAP) pvc
Manacapuru, Amazonas, Brazil; Mobile Facility (GOAMAZON) mao
Point Reyes CA, USA; Mobile Facility (MASRAD) pye
Ganges Valley, India; Mobile Facility (GVAX) pgh
Shouxian, Anhui, China; Mobile Facility hfe
Graciosa Island, Azores, Portugal; Mobile Facility (CAP-MPL) grw
Black Forest, Germany; Mobile Facility (COPS) fkb
Niamey, Niger; Mobile Facility (RADAGAST) nim
Ascension Island, South Atlantic Ocean; Mobile Facility (LASIC) asi
Oliktok Point, Alaska, USA; Mobile Facility oli
AWARE (ARM West Antarctic Radiation Experiment) awr
Hyytiala, Finland; Mobile Facility (BAECC) tmp
Steamboat Springs CO, USA; Mobile Facility (STORMVEX) sbs
MAGIC (Marine ARM GPCI Investigation of Clouds); Mobile Facility mag
ACAPEX (ARM Cloud Aerosol Precip Experiment); Mobile Facility acx
Gan Island, Maldives; Mobile Facility (AMIE-GAN) gan
MARCUS (Shipboard Southern Ocean Campaign); Mobile Facility mar

Intermediate Sites
McClellan AFB, Sacramento, CA, USA; Off-Site Campaign (CARES) mcc
Macquarie Island, SW Pacific Ocean; Off-Site Campaign (MICRE) mcq
Deadhorse Airport, Prudhoe Bay, AK; Off-Site Campaign (ACME-V) scc
SHEBA (Surface HEat Budget of the Arctic) shb

External Sites
 Global Earth Coverage gec
'''

if __name__ == "__main__":
    main()