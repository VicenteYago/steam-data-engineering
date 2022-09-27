
import sys
import re
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.functions import input_file_name
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

def proc_json(raw_df, field)  :
    rows = raw_df[field]
    return(rows)

def get_previous_word(text):
    matches = re.search('.*/(\d+)-.*', text)
    return matches.group(1)

extract_game_id = F.udf(
    lambda text: get_previous_word(text),
    StringType()
)


GCP_PROJECT_ID = sys.argv[1]
BQ_DATASET     = sys.argv[2]
BQ_TABLE       = sys.argv[3]
BUCKET         = sys.argv[4] 
BUCKET_SUBDIR  = sys.argv[5]
TEMP_BUCKET    = sys.argv[6]

def main():

    conf = SparkConf() \
        .setAppName('steam-gcp-dataproc') \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") 

    sc = SparkContext(conf=conf)

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()


    # READ DATA
    all_games = spark.read.json(f"gs://{BUCKET}/{BUCKET_SUBDIR}/*", multiLine=True) \
                        .withColumn("filename", input_file_name())

    rdd = all_games.rdd
    rdd = rdd.repartition(5)


    # REVIEWS
    df_reviews = rdd.flatMap(lambda item : proc_json(item, field = "reviews")) \
                    .toDF()

    # GAMEID
    df_gameid = rdd.map(  lambda item : [ proc_json(item, field = "reviews"),
                                        proc_json(item, field = "filename")] ) \
                .flatMap(lambda item:  [item[1] for i in item[0] ]) \
                .map(lambda item : Row(gameid  = item, )).toDF()

    df_gameid = df_gameid.withColumn('gameid', extract_game_id('gameid'))

    # JOIN DATAFRAMES
    w = Window().orderBy(lit('A'))
    df_reviews = df_reviews.withColumn("row_num", row_number().over(w))
    df_gameid = df_gameid.withColumn("row_num", row_number().over(w))

    df_reviews = df_reviews.join(df_gameid, on = ["row_num"], how = "inner")

    df_reviews_flat = flatten_df(df_reviews) 

    # BIGQUERY WRITE
    df_reviews_flat.write \
    .format('bigquery') \
    .option('table', f'{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}') \
    .mode("append") \
    .option("temporaryGcsBucket",TEMP_BUCKET) \
    .save()



if __name__ == "__main__":
    main()
