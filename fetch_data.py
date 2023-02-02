import json
import os
import pandas as pd
import numpy as np
import warnings
from datetime import date
from searchtweets import gen_request_parameters, load_credentials, collect_results

warnings.filterwarnings("ignore")

HOME = os.path.expanduser('~')
print(HOME)
DATALAKE_ROOT_FOLDER = HOME + "/Desktop/M2/S1/Data Lake/projet data lake/datalake_data/"


def get_games_names(chemin):
    data = pd.read_csv(chemin, sep=';')
    # print(data)
    print(data.columns)
    game_names = list(np.array(data['Name']))
    print(len(game_names))
    # valeur unique
    game_name_unique = []
    for name in game_names:
        if name not in game_name_unique:
            game_name_unique.append(name)

    print(len(game_name_unique))
    return game_name_unique

def query_data_from_twitter(game_name):
    query = gen_request_parameters(game_name, None, results_per_call=10)
    print("We are getting data from Twitter ...", query)
    config_file = str(HOME) + "/twitter_keys.yaml"
    search_args = load_credentials(config_file, yaml_key="search_tweets_v2", env_overwrite=False)
    return collect_results(query, max_tweets=10, result_stream_args=search_args)

def store_twitter_data(tweets, game_name):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeu/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
    #print("Writing here: ", TARGET_PATH)
    #f = open(TARGET_PATH + str(game_name) + ".json", "w+")
    #f.write(json.dumps(tweets, indent=4))
    #return pd.read_json('tweets.json', encoding='utf-8')
    try:
        df = pd.DataFrame(tweets[0]['data'])
        # add a column of index based on game name !! hash
        df["Game_name"] = game_name
        df["index_hash"] = hash(game_name)
        # df["key"] = hash(game_name)
        return df
    except:
        return pd.DataFrame()
    #print(df.columns)

def fetch_data_from_twitter(chemin, current_day):
    # obtenir la liste des noms uniques
    names = get_games_names(chemin)
    names = names[:100]
    # fetch data for each name in names
    dat = pd.DataFrame()
    for game_name in names:
        # formulation de l'enoncé de la requete : nom avec retweets
        # n1 = n+" -is:retweet -has:media"
        tweets = query_data_from_twitter(game_name)
        df = store_twitter_data(tweets, game_name)
        # print(df)
        dat = dat.append(df, ignore_index=True)
    print(dat)
    twitter_json = json.dumps(dat.to_dict(), indent=4)
    print(twitter_json)
    # sauvegarde
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeu/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    print("Writing here: ", TARGET_PATH)
    f = open(TARGET_PATH + "twitter_comments.json", "w+")
    f.write(twitter_json)
    f.close()

"""
def store_kaggle_data(chemin):
    current_day = date.today().strftime("%Y%m%d")

    # get game_names
    games_names = get_games_names(chemin)
    print("il y a :", len(games_names), " jeux distincts")
    # importer le dataset .csv
    df = pd.read_csv(chemin, sep=";")
    # création+export des dicts/json
    # for game_name in games_names:
    n = len(df.index)
    count = 0  # nombre d'echecs
    print("Writing...")
    for k in range(n):
        ind = []
        df_jeu = pd.DataFrame()
        if df["Name"][k] in games_names:
            game_name = df["Name"][k]
            # subset
            df_jeu = df[df["Name"] == game_name]
            df_jeu = df_jeu.to_dict()  # convertir en dico
            # conv en j_son
            # preparation pour la conversion
            liste_vide = []
            liste_vide.append(df_jeu)
            dico_vide = {}
            dico_vide["data"] = liste_vide
            data_jeu = [dico_vide]
            # exportation/ecriture en json
            TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/kaggle/Jeu/" + current_day + "/"
            if not os.path.exists(TARGET_PATH):
                os.makedirs(TARGET_PATH)
            # print("Writing here: ", TARGET_PATH, str(game_name) + ".json")
            try:
                f = open(TARGET_PATH + str(game_name) + ".json", "w+")
                f.write(json.dumps(data_jeu, indent=4))
            except:
                if str(game_name).__contains__("\\and\\"):
                    correct_name = " ".join(game_name.split("\\"))
                    try:
                        f = open(TARGET_PATH + correct_name + ".json", "w+")
                        f.write(json.dumps(data_jeu, indent=4))
                    except:
                        count += 1
    print("Nombre d'échecs d'écriture: ", count)
"""

def convert_raw_to_formatted_twitter(file_name, current_day):
    RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeu/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/twitter/Jeu/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)

    df = pd.read_json(RATING_PATH)
    print(df)
    if df.empty:
        print(file_name + " is empty ! no comment")
        pass
    else:
        print("Converting twitter comments to parquet...")
        parquet_file_name = file_name.replace(".json", ".snappy.parquet")
        # final_df = pd.DataFrame(data=df.data)
        print("Converting here : ", FORMATTED_RATING_FOLDER + parquet_file_name)
        df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
        print("Conversion twitter ok !")

def convert_raw_to_formatted_kaggle(file_name, current_day):
    # RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/kaggle/Jeu/" + current_day + "/"
    RATING_PATH = file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/kaggle/Jeu/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)

    print("Conversion kaggle vgsales to parquet...")
    df = pd.read_csv(RATING_PATH, sep=";")
    df["index"] = [hash(n) for n in df["Name"]]
    parquet_file_name = file_name.replace(".csv", ".snappy.parquet")
    print("Converting here : ", FORMATTED_RATING_FOLDER + parquet_file_name)
    df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
    print("Conversion kaggle ok !")

if __name__ == "__main__":
    # on récupère les données de Kaggle et on en fait une liste unique des noms des jeux
    # names = get_games_names("vgsales_2.csv")
    # print(names)

    day = date.today().strftime("%Y%m%d")
    # on récupère les données de l'API twitter
    fetch_data_from_twitter("vgsales_2.csv", day)

    # ensuite on passe le fichier récupéré dans le raw_to_format pour obtenir le parquet
    convert_raw_to_formatted_twitter(file_name="twitter_comments.json", current_day=day)

    # enfin convertir le fichier raw (.csv) de kaggle vers le format snappy parquet
    convert_raw_to_formatted_kaggle(file_name="vgsales_2.csv", current_day=day)