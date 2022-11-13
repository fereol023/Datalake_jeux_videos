import json
from datetime import date
import os

from searchtweets import gen_request_parameters, load_credentials, collect_results

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/workspace/datalake_data/"


def fetch_data_from_twitter(**kwargs):
   tweets = query_data_from_twitter()
   store_twitter_data(tweets)


def query_data_from_twitter():
   query = gen_request_parameters("#clashofclans", None, results_per_call=100)
   print("We are getting data from Twitter ...", query)
   config_file = str(HOME)+"/twitter_keys.yaml"
   search_args = load_credentials(config_file, yaml_key="search_tweets_v2", env_overwrite=False)
   return collect_results(query, max_tweets=100, result_stream_args=search_args)


def store_twitter_data(tweets):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeux/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)
   print("Writing here: ", TARGET_PATH)
   f = open(TARGET_PATH + "twitter.json", "w+")
   f.write(json.dumps(tweets, indent=4))

#fetch_data_from_twitter()
