from pyspark import SparkContext, SparkConf
from operator import add
from fileinput import input
from glob import glob
from tempfile import NamedTemporaryFile

conf = SparkConf().setAppName("main").setMaster("local[*]")
sc = SparkContext(conf=conf)

album_rdd = sc.textFile("data/albums.csv")
artist_rdd = sc.textFile("data/artists.csv")
 
album_headers = ["id", "artist_id", "album_title", "genre", "year_of_pub", "num_of_tracks", "num_of_sales", "rolling_stone_critic", "mtv_critic", "music_maniac_critic"]
artist_headers= ["id", "real_name", "art_name", "role", "year_of_birth", "country", "city", "email", "zip_code"]

def split_and_map(line, header):
    my_list = line.split(",")
    return dict(zip(header, my_list))

# Task 1

def get_all_genres(rdd):
    return rdd \
        .map(lambda line: split_and_map(line, album_headers)) \
        .map(lambda album: album["genre"]) \
        .distinct() \
        .collect()

# Task 2

def get_oldest_artist_birth_year(rdd):
    return  rdd \
      .map(lambda line: split_and_map(line, artist_headers)) \
      .map(lambda artist: artist["year_of_birth"]) \
      .max()
    
    print(result)

# Task 3

def get_amount_of_artists_by_country(rdd):
    result = rdd \
      .map(lambda line: split_and_map(line, artist_headers)) \
      .map(lambda artist: (artist["country"], 1)) \
      .reduceByKey(add) \
      .sortByKey() \
      .sortBy(ascending=False, keyfunc=lambda t: t[1]) \
      .map(lambda t: '{0}\t{1}'.format(t[0], t[1]))
        
    return result.collect()

def write_result_to_file(rdd, filename):
    with open('result/{}'.format(filename), 'w') as f:
        f.write("\n".join(rdd))

# Task 4

def album_per_artist(rdd):
    return(rdd
           .map(lambda line: split_and_map(line, album_headers))
           .groupBy(lambda artist: artist["artist_id"])
           .mapValues(list)
           .mapValues(len)
           .sortBy(lambda x: int(x[0]))
           .sortBy(lambda x: -x[1])
           .collect())
