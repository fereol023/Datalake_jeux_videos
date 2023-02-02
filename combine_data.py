from __future__ import print_function
import os
import warnings
from datetime import date
from pyspark.sql import SQLContext
warnings.filterwarnings("ignore")

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-19"
os.environ["SPARK_HOME"] = r"C:\Users\gbeno\Documents\spark-3.3.1-bin-hadoop3"

import findspark
findspark.init()
findspark.find()

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/Desktop/M2/S1/Data Lake/projet data lake/datalake_data/"


def combine_data(current_day):
    print("Combining...")

    # créer les chemins
    COMMENTS_PATH_twitter = DATALAKE_ROOT_FOLDER + "formatted/twitter/Jeu/" + current_day + "/"
    SALES_PATH_kaggle = DATALAKE_ROOT_FOLDER + "formatted/kaggle/Jeu/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/JeuAnalysis/JeuStatistics/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER + "usage/JeuAnalysis/JeuTop10/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_FULL = DATALAKE_ROOT_FOLDER + "usage/JeuAnalysis/JeuFull_df/" + current_day + "/"

    if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
        os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
        os.makedirs(USAGE_OUTPUT_FOLDER_BEST)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_FULL):
        os.makedirs(USAGE_OUTPUT_FOLDER_FULL)

    from pyspark import SparkContext

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    df_vgsales = sqlContext.read.parquet(SALES_PATH_kaggle)
    df_vgsales.registerTempTable("vgsales")

    # Check content of the DataFrame df_sales:
    print("df_vgsales")
    df_vgsales.show()

    df_comments = sqlContext.read.parquet(COMMENTS_PATH_twitter)
    df_comments.registerTempTable("comments")

    # Check content of the DataFrame df_ratings:
    print("df_comments")
    df_comments.show()

    # sql requests on our data
    stats_df = sqlContext.sql("SELECT AVG(Global_Sales) AS avg_sales,"
                              "       MAX(Global_Sales) AS max_sales,"
                              "       MIN(Global_Sales) AS min_sales"
                              "    FROM vgsales LIMIT 10")
    top10_df = sqlContext.sql("SELECT Genre, EU_Sales"
                              "    FROM vgsales"
                              "    WHERE EU_Sales > 10"
                              "    ORDER BY EU_Sales DESC"
                              "    LIMIT 10")

    # comments + sales ==> à utiliser dans ELK

    df_vgsales.createOrReplaceTempView("A")
    df_comments.createOrReplaceTempView("B")
    full_df = sqlContext.sql("SELECT Game_name, Platform, Publisher, text as Tweets, EU_Sales, NA_Sales, JP_Sales, "
                             "Other_Sales, Global_Sales "
                             " FROM B"
                             " JOIN A on B.index_hash=A.index")
    # Check content of stats_df and save it:
    print("stats_df")
    stats_df.show()
    stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")

    # Check content of the DataFrame top10_df  and save it:
    print("top10_df")
    top10_df.show()
    stats_df.write.save(USAGE_OUTPUT_FOLDER_BEST + "res.snappy.parquet", mode="overwrite")

    # Check content of the DataFrame full_df  and save it:
    print("combining query")
    full_df.show()
    full_df.write.save(USAGE_OUTPUT_FOLDER_FULL + "res.snappy.parquet", mode="overwrite")


if __name__ == "__main__":
    day = date.today().strftime("%Y%m%d")
    combine_data(day)


