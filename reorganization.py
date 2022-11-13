import json
from datetime import date
import os
import pandas as pd
import numpy as np

from searchtweets import gen_request_parameters, load_credentials, collect_results

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/Desktop/M2/S1/Data Lake/projet data lake/datalake_data/"

#
def get_games_names (chemin):
    data = pd.read_csv(chemin, sep=';')
    #print(data)
    print(data.columns)
    game_names=list(np.array(data['Name']))
    print(len(game_names))
    # valeur unique
    game_name_unique=[]
    for name in game_names:
        if name not in game_name_unique:
            game_name_unique.append(name)

    print(len(game_name_unique))
    return game_name_unique


def fetch_data_from_twitter(chemin):
    # obtenir la liste des noms uniques
    names = get_games_names(chemin)
    names = names[:100]
    # fetch data for each name in names
    for n in names:
        # formulation de l'enoncé de la requete : nom avec retweets
        #n1 = n+" -is:retweet -has:media"
        tweets = query_data_from_twitter(n)
        store_twitter_data(tweets, n)


def query_data_from_twitter(game_name):
   query = gen_request_parameters(game_name, None, results_per_call=10)
   print("We are getting data from Twitter ...", query)
   config_file = str(HOME)+"/twitter_keys.yaml"
   search_args = load_credentials(config_file, yaml_key="search_tweets_v2", env_overwrite=False)
   return collect_results(query, max_tweets=10, result_stream_args=search_args)


def store_twitter_data(tweets, game_name):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeu/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)
   print("Writing here: ", TARGET_PATH)
   f = open(TARGET_PATH + str(game_name)+".json", "w+")
   f.write(json.dumps(tweets, indent=4))

# fonction qui prend les fichier csv vgsales et fait un split par game_names et exporte les fichiers en .json
def store_kaggle_data(chemin) :
    current_day = date.today().strftime("%Y%m%d")

    # get game_names
    games_names = get_games_names(chemin)
    print("il y a :", len(games_names), " jeux distincts")
    # importer le dataset .csv
    df = pd.read_csv(chemin, sep=";")
    # création+export des dicts/json
    #for game_name in games_names :
    n = len(df.index)
    for k in range(n) :
        ind = []
        df_jeu = pd.DataFrame()
        if df["Name"][k] in games_names :
            game_name = df["Name"][k]
            # subset
            df_jeu = df[df["Name"] == game_name]
            df_jeu = df_jeu.to_dict() #convertir en dico
            # conv en j_son
            # exportation/ecriture en json
            TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/kaggle/Jeu/" + current_day + "/"
            if not os.path.exists(TARGET_PATH):
                os.makedirs(TARGET_PATH)
            print("Writing here: ", TARGET_PATH)
            f = open(TARGET_PATH + str(game_name) + ".json", "w+")
            f.write(json.dumps(df_jeu, indent=4))



def convert_raw_to_formatted_twitter(file_name, current_day):

    RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeu/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/twitter/Jeu/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)
    df = pd.read_json(RATING_PATH)
    # on convertit uniquement les fichiers non vides
    #print(type(df))
    if df.empty :
        print(file_name+" is empty ! no comment")
        pass
    else :
        parquet_file_name = file_name.replace(".json", ".snappy.parquet")
        final_df = pd.DataFrame(data=df.data)
        final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)


def convert_raw_to_formatted_kaggle(file_name, current_day):

    RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/kaggle/Jeu/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/kaggle/Jeu/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)
    df = pd.read_json(RATING_PATH)
    # on convertit uniquement les fichiers non vides
    #print(type(df))
    if df.empty :
        print(file_name+" is empty ! no comment")
        pass
    else :
        parquet_file_name = file_name.replace(".json", ".snappy.parquet")
        final_df = pd.DataFrame(data=df.data)
        final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)

if __name__ == "__main__":

    # on recupère les données de Kaggle et on en fait une liste unique des noms des jeux
    #names = get_games_names("vgsales.csv")
    #print(names)

    # on récupere les données de l'API twitter
    #fetch_data_from_twitter("vgsales.csv")

    # ensuite on passe les fichiers récupérés dans le raw_to_format pour obtenir le parquet
    #day = date.today().strftime("%Y%m%d")
    #filenames = os.listdir(r"C:\Users\gbeno\Desktop\M2\S1\Data Lake\projet data lake\datalake_data\raw\twitter\Jeu\20221026")
    #print(filenames)

    #for n in filenames :
        #print(n, day)
        #convert_raw_to_formatted(file_name=n, current_day=day)

    store_kaggle_data("vgsales.csv")






























