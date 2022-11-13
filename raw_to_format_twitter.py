
import os
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/Desktop/M2/S1/Data Lake/projet data lake/datalake_data/"

def convert_raw_to_formatted(file_name, current_day):

    RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/twitter/Jeu/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/twitter/Jeu/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)

    df = pd.read_json(RATING_PATH)
    parquet_file_name = file_name.replace(".json", ".snappy.parquet")
    final_df = pd.DataFrame(data=df.data)
    final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)

#test. Change the arguments to match the files of your data lake
convert_raw_to_formatted("twitter.json", "20221004")