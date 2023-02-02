import json
import os

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import date

ES_NODES = "http://localhost:9200"


def load_df(filepath):
    # load
    df = pd.read_parquet(filepath)
    df["index_hash"] = [hash(n) for n in df["Game_name"]]
    return df


def add_timestamp(dataframe, day_date):
    dataframe["timestamp"] = day_date
    return dataframe


def upload_data(index_name, doc_type, df, node, day_date):

    # == PREPARATION (MISE EN FORME) + ENVOI
    liste_hash_jeux = list(set(df["index_hash"]))
    for hash_jeu in liste_hash_jeux:
        # ============ PREPARATION ===============
        # on fait le traitement pour chaque nom de jeu
        # E1 : récupérer les lignes qui concernent le jeu
        df_i = df[df["index_hash"] == hash_jeu]
        # prendre le nom du jeu (utile plus tard)
        nom_du_jeu = df_i["Game_name"].values[0]
        # E2 : convertir le df en dico
        D2 = df_i.to_dict(orient="list")
        # E3 : prendre les informations du jeu
        record_jeu = {
            "Game_name": list(set(D2["Game_name"])),
            "Platform": list(set(D2["Platform"])),
            "Publisher": list(set(D2["Publisher"])),
            "Tweets": list(set(D2["Tweets"])),
            "EU_Sales": list(set(D2["EU_Sales"])),
            "NA_Sales": list(set(D2["NA_Sales"])),
            "JP_Sales": list(set(D2["JP_Sales"])),
            "Other_Sales": list(set(D2["Other_Sales"])),
            "Global_Sales": list(set(D2["Global_Sales"])),
            "TimeStamp": list(set(D2["timestamp"])),
            "index_hash": list(set(D2["index_hash"]))
        }

        # E4 : créer le dico à envoyer
        meta_dict = {
            "index": {
                "_index": index_name,
                "_type": doc_type,
                "_timestamp": day_date,
                "_data": record_jeu
            }
        }

        # ============ INDEXING TO ELK ===============
        print("Indexing : " + nom_du_jeu)
        es_client = Elasticsearch(hosts=node)
        # print(es_client.info())
        print("=" * 50)
        es_client.index(
            index=index_name,
            id=hash_jeu,
            body=meta_dict)
        print("Ok !")


def index_es_data(index_name, es_data, node):
    print("Indexing : " + str(index_name))
    es_client = Elasticsearch(hosts=node)
    # print(es_client.info())
    print("=" * 50)
    es_client.index(
        index=index_name,
        body=es_data)
    print("Ok !")

    """
    if es_client.indices.exists(index):
        print("deleting the '{}' index.".format(index))
        res = es_client.indices.delete(index=index)
        print("Response from server: {}".format(res))

    print("creating the '{}' index.".format(index))
    res = es_client.indices.create(index=index)
    print("Response from server: {}".format(res))

    print("bulk index the data")
    res = es_client.bulk(index=index, body=es_data, refresh=True)
    print("Errors: {}, Num of records indexed: {}".format(res["errors"], len(res["items"])))
    
    if es_client.indices.exists(index_name):
        print("deleting the '{}' index.".format(index_name))

    #helpers.bulk(client, docs, index="vgsales", doc_type="_doc")
    """


def request_on(index_name, node):
    es_client = Elasticsearch(hosts=node)
    # check loading
    result = es_client.search(
        index=index_name,
        body={
            "query": {
                "match_all": {}
            }
        }
    )
    json_object = json.loads(json.dumps(result))
    json_formatted_str = json.dumps(json_object, indent=2)
    print(json_formatted_str)
    # print(result)


if __name__ == "__main__":

    day = date.today().strftime("%Y%m%d")

    # LECTURE DU FICHIER SPARK DU JOUR
    ROOT = "C:/Users/gbeno/Desktop/M2/S1/Data Lake/projet data lake/datalake_data/usage/JeuAnalysis/JeuFull_df/"
    files = os.listdir(ROOT + str(day) + "/res.snappy.parquet")
    filename = [f for f in files if f.endswith(".snappy.parquet")]
    filename = ROOT + str(day) + "/res.snappy.parquet/" + filename[0]

    df_full = load_df(filename)
    df_full = add_timestamp(df_full, day)
    print(df_full)

    # CHARGEMENT SUR LE NODE ELK
    upload_data(index_name="full_data4", doc_type="VGSALES_COMMENT", df=df_full, node=ES_NODES, day_date=day)
    print("=" * 50)
    request_on("full_data4", ES_NODES)

    # FIN

    """
    # CHARGEMENT PAR HASH_JEU
    for hash_jeu in hash_jeux:
        es_data = prepare_es_data(index="idx" + str(hash_jeu), doc_type="sales", df=df_full[df_full["index_hash"] == hash_jeu])
        # print(es_data)
        # print("=" * 50)
        index_es_data(index_name="idx" + str(hash_jeu), es_data=es_data, node=ES_NODES)

    # After upload check loading for
    test_idx = 'idx6087654461939211256'
    request_on(test_idx, ES_NODES)
    ## FIN CHARGEMENT
    """

    # AUTRES TESTS
    """
    print("+++++++++++++++++++++++++++++++++++++++++")
    #D = df_full[df_full["index_hash"] == hash_jeux[1]]
    D = df_full[df_full["Game_name"] == "Just Dance"]
    D2 = D.to_dict(orient="list")

    import json
    print(json.dumps(D2, indent=2))
    a_charger = {}
    a_charger = {
        "Game_name": list(set(D2["Game_name"])),
        "Platform": list(set(D2["Platform"])),
        "Publisher": list(set(D2["Publisher"])),
        "Tweets": list(set(D2["Tweets"])),
        "EU_Sales": list(set(D2["EU_Sales"])),
        "NA_Sales": list(set(D2["NA_Sales"])),
        "JP_Sales": list(set(D2["JP_Sales"])),
        "Other_Sales": list(set(D2["Other_Sales"])),
        "Global_Sales": list(set(D2["Global_Sales"])),
        "index_hash": list(set(D2["index_hash"]))
    }

    record2 = {

    }

    print("="*50)
    print(json.dumps(a_charger, indent=3))
    """
