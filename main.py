'''This module does data aggregation using spark'''
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


CONF = SparkConf().setAppName("main").setMaster("local[*]")
SC = SparkContext(conf=CONF)

ALBUM_HEADERS = ["id", "artist_id", "album_title", "genre", "year_of_pub"
                 , "num_of_tracks", "num_of_sales", "rolling_stone_critic",
                 "mtv_critic", "music_maniac_critic"]

ARTIST_HEADERS = ["id", "real_name", "art_name", "role", "year_of_birth"
                  , "country", "city", "email", "zip_code"]

def split_and_map(line, header):
    '''Splits string line and zips into a dict with appropriate headers'''
    my_list = line.split(",")
    return dict(zip(header, my_list))

def identity(a):
    '''Returns itself'''
    return a

def intersperse(iterable, delimiter):
    '''Intersperes a delimiter between all elements in an iterable'''
    it = iter(iterable)
    yield next(it)
    for x in it:
        yield delimiter
        yield x

ALBUM_RDD = SC.textFile("data/albums.csv").map(lambda line: split_and_map(line, ALBUM_HEADERS))
ARTIST_RDD = SC.textFile("data/artists.csv").map(lambda line: split_and_map(line, ARTIST_HEADERS))

# Task 1

def get_all_genres(rdd):
    '''Returns all distinct genres'''
    return(rdd
           .map(lambda album: album["genre"])
           .distinct()
           .collect())

# Task 2

def get_oldest_artist_birth_year(rdd):
    '''Returns the oldest artists year of birth'''
    return(rdd
           .map(lambda artist: int(artist["year_of_birth"]))
           .min())

# Task 3

def get_amount_of_artists_by_country(rdd):
    '''Returns a list of touples contianing the amount of artists from each country'''
    return(rdd
           .map(lambda artist: (artist["country"], 1))
           .reduceByKey(add)
           .sortByKey()
           .sortBy(ascending=False, keyfunc=lambda t: t[1])
           .collect())

def write_result_to_file(data, filename, deep=True):
    '''Helper function for writing results to file'''
    if deep:
        str_list = list(map(lambda x: ''.join(intersperse(map(str, x), '\t')), data))
    else:
        str_list = list(map(str, data))

    with open('result/{}'.format(filename), 'w') as f:
        f.write('\n'.join(str_list))
    print("File has been written")

# Task 4

def album_per_artist(rdd):
    '''Returns sorterd list of touples containing amount of albums per artist'''
    return(rdd
           .map(lambda album: (int(album["artist_id"]), int(album["id"])))
           .groupBy(lambda t: t[0])
           .mapValues(list)
           .map(lambda t: t[1])
           .flatMap(identity)
           .reduceByKey(add)
           .sortBy(lambda x: int(x[0]))
           .sortBy(lambda x: -x[1])
           .collect())

# Task 5

def sales_per_genre(rdd):
    '''Returns sorted list of touples containing amount of sales per genre'''
    return(rdd
           .map(lambda album: (album["genre"], int(album["num_of_sales"])))
           .groupBy(lambda album: album[0])
           .mapValues(list)
           .map(lambda t: t[1])
           .flatMap(lambda x: x)
           .reduceByKey(add)
           .sortBy(lambda x: x[0])
           .sortBy(lambda x: -x[1])
           .collect())

# Task 6

def average_score(album):
    '''Helper function for calculating average critic score'''
    total = float(album["rolling_stone_critic"]) \
          + float(album["mtv_critic"]) \
          + float(album["music_maniac_critic"])

    return total/3

def best_average_critic(rdd):
    '''Returns top ten list of touples with best average critic score'''
    return(rdd
           .map(lambda album: (int(album["id"]), average_score(album)))
           .takeOrdered(10, key=lambda x: -x[1]))

# Task 7

def best_average_critic_and_country(album_rdd, artist_rdd):
    '''Returns top ten list of lists with best average critic score'''
    albums = SC.parallelize(
        album_rdd
        .map(lambda album: (int(album["artist_id"]), (int(album["id"]), average_score(album))))
        .takeOrdered(10, key=lambda x: -x[1][1]))

    artists = artist_rdd.map(lambda artist: (int(artist["id"]), artist["country"]))

    return(albums
           .join(artists)
           .map(lambda t: [t[1][0][0], t[1][0][1], t[1][1]])
           .collect())

# Task 8

def artist_with_highest_album(album_rdd, artist_rdd):
    '''Returns sorted list of strings containing artists with highest mtv_critic score'''
    albums = album_rdd.map(
        lambda album: (int(album["artist_id"]), (int(album["id"]), float(album["mtv_critic"]))))
    artists = artist_rdd.map(lambda artist: (int(artist["id"]), artist["art_name"]))

    # (artist_id, ((album_id, mtv_critic), artist_name))
    return(albums
           .join(artists)
           .filter(lambda t: t[1][0][1] == 5.0)
           .map(lambda t: t[1][1])
           .distinct()
           .sortBy(identity)
           .collect())

# Task 9

def average_artist_critic_norway(album_rdd, artist_rdd):
    '''Returns sorted list of average artist score for all norwegian artists'''
    artists = artist_rdd \
      .filter(lambda artist: artist["country"] == "Norway") \
      .map(lambda artist: (int(artist["id"]), artist["art_name"]))

    albums = album_rdd.map(lambda album: (int(album["artist_id"]), float(album["mtv_critic"])))

    point = albums.join(artists).groupByKey().mapValues(len)

    return(albums
           .join(artists)
           .reduceByKey(lambda a, b: (a[0] + b[0], a[1]))
           .join(point)
           .map(lambda t: [t[1][0][1], "Norway", t[1][0][0]/t[1][1]])
           .collect())

SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()

ALBUM_DF = SPARK.createDataFrame(ALBUM_RDD)
ARTIST_DF = SPARK.createDataFrame(ARTIST_RDD)

# Task 10
def get_distict_artists(df):
    '''Get amount of distinct artists'''
    return df.agg(approx_count_distinct(df.id).alias("distinct_artists")).show()
    
def get_distinct_albums(df):
    '''Get amount of distinct albums'''
    return df.agg(approx_count_distinct(df.id).alias("distinct_albums")).show()

def get_distinct_genres(df):
    '''Get amount of distinct genres'''
    return df.agg(approx_count_distinct(df.genre).alias("distinct_genres")).show()

def get_distinct_countries(df):
    '''Get amount of distinct countries'''
    return df.agg(approx_count_distinct(df.country).alias("distinct_countries")).show()

def min_year_of_pub(df):
    '''Get oldest year of publication'''
    return df.agg(min(df.year_of_pub)).show()

def max_year_of_pub(df):
    '''Get latest year of publication'''
    return(df.agg(max(df.year_of_pub)).show())

def min_year_of_birth(df):
    '''Get oldest year of birth'''
    return(df.agg(min(df.year_of_birth)).show())

def max_year_of_birth(df):
    '''Get youngest year of birth'''
    return(df.agg(max(df.year_of_birth)).show())

def task_10():
    '''Prints out all the results for task 10'''
    get_distict_artists(ARTIST_DF)
    get_distinct_albums(ALBUM_DF)
    get_distinct_genres(ALBUM_DF)
    get_distinct_countries(ARTIST_DF)
    min_year_of_pub(ALBUM_DF)
    max_year_of_pub(ALBUM_DF)
    min_year_of_birth(ARTIST_DF)
    max_year_of_birth(ARTIST_DF)
