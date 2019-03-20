'''This module does data aggregation using spark on tweets'''
import sys
from functools import reduce
from pyspark import SparkConf, SparkContext

CONF = SparkConf() \
  .setAppName("main") \
  .setMaster("local[*]") \
 # .setExecutorEnv('spark.driver.memory', '14g') \
 # .setExecutorEnv('spark.executor.memory', '14g')
SC = SparkContext(conf=CONF)

TWITTER_HEADERS = ["user", "tweet_text"]
#TWITTER_RDD = SC.textFile("data/tweets.tsv").map(lambda line: split_and_map(line, TWITTER_HEADERS))

def split_to_touple(line):
    '''Splits line into touple'''
    line_list = line.split('\t')
    return (line_list[0], line_list[1])

TWITTER_RDD = SC.textFile("data/tweets.tsv").map(split_to_touple)

# Define helper functions for manipulating data and writing to file

def split_and_map(line, header):
    '''Splits string line and zips into a dict with appropriate headers'''
    my_list = line.split('\t')
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

def write_result_to_file(data, filename, deep=True):
    '''Helper function for writing results to file'''
    if deep:
        str_list = list(map(lambda x: ''.join(intersperse(map(str, x), '\t')), data))
    else:
        str_list = list(map(str, data))

    with open('result/{}'.format(filename), 'w') as f:
        f.write('\n'.join(str_list))
    print("File has been written")

def freq(word: str, tweet_words: list):
    '''Takes user x's total amount of tweets and a set of the
    union of words between user x and y, then returns the frequency'''
    return tweet_words.count(word)

def sim_score(word_set: set, tweets_x: list, tweets_y: list):
    '''Calculates the similarity score betweeen two users'''
    return reduce(
        lambda a, b: min(freq(a, tweets_x), freq(a, tweets_y))
        + min(freq(b, tweets_x), freq(b, tweets_y))
        , word_set)

def get_user_words(rdd, user):
    '''Return list of all distinct user words from tweets'''
    step = rdd.filter(lambda row: row[0] == user)
    if step.count() <= 0:
        raise ValueError("User doesn't exist in file")
    else:
        return(step
               .map(lambda row: row[1])
               .flatMap(lambda val: val.split())
               .distinct()
               .collect())

flatten = lambda l: [item for sublist in l for item in sublist]

# rdd = SC.parallelize([("meme", "he she tree lee fee ree"), ("nope", "top fope lee ree never ever"), ("scope", "tope rope lee tree she"), ("nope", "never ever lever top he")])

def sim_usrs(rdd, user="tmj_bos_hr", k=0):
    '''Test'''
    #user_words = get_user_words(rdd, user)

    return(rdd
           .filter(lambda row: not row[0] == user)
           .mapValues(lambda s: s.split())
           #.reduceByKey(lambda x, y: x + y) #(user, [tweet_string])
           #.mapValues(lambda t: (t, set(user_words).union(t), len(set(user_words).intersection(t)))) #(user, ([tweet_words], {tweet_words_union_set}, common_words_count))
           #.mapValues(lambda t: (sim_score(t[1], user_words, t[0]), t[2])) #(user, (sim_score, common_words_count))
           .collect())

def similar_users(user, k, file_path, output_file):
    '''Writes similar users to file'''
    conf = SparkConf().setAppName("main").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    twitter_headers = ["user", "tweet_text"]
    twitter_rdd = sc.textFile(file_path).map(lambda line: split_and_map(line, twitter_headers))


def main(argv):
    print(argv)

if __name__ == "__main__":
    main(sys.argv)
