'''This module does data aggregation using spark on tweets'''
import sys
from functools import reduce
from pyspark import SparkConf, SparkContext

CONF = SparkConf() \
  .setAppName("main") \
  .setMaster("local[*]") \
  .setExecutorEnv('spark.driver.memory', '5g') \
  .setExecutorEnv('spark.executor.memory', '3g')
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

def freq(word, tweet_words):
    '''Takes user x's total amount of tweets and a set of the
    union of words between user x and y, then returns the frequency'''
    return tweet_words.count(word)

def sim_score(tweets_x, tweets_y):
    '''Calculates the similarity score betweeen two users'''
    word_set = set().union(tweets_x, tweets_y)
    my_sum = 0
    for word in word_set:
        my_sum += min(freq(word, tweets_x), freq(word, tweets_y))

    return my_sum

    '''return reduce(
        lambda a, b: min(freq(a, tweets_x), freq(a, tweets_y))
        + min(freq(b, tweets_x), freq(b, tweets_y))
        , word_set)'''

def get_user_words(rdd, user):
    '''Return list of all distinct user words from tweets'''
    step = rdd.filter(lambda row: row[0] == user)
    if step.count() <= 0:
        raise ValueError("User doesn't exist in file")
    else:
        return(step
               .values()
               .flatMap(lambda val: val.split()) #[user_words]
               #.distinct()
               .collect())

def get_union_words(rdd, queried_user_words, other_user):
    return get_user_words(rdd, other_user).union(queried_user_words)

# rdd = SC.parallelize([("meme", "he she tree lee fee ree"), ("nope", "top fope lee ree never ever"), ("scope", "tope rope lee tree she"), ("nope", "never ever lever top he")])

def rmd_usrs(rdd, user="tmj_bos_hr", k=3):
    '''Returns list of recommended users'''
    tweet_words = intern_rdd(rdd, user)
    union_rows = union_rdd(rdd, user)
    queried_user_words = get_user_words(rdd, user)

    return(rdd
           .filter(lambda row: not row[0] == user)
           .mapValues(lambda s: s.split()) #(user, [tweet_words])
           #.reduceByKey(lambda a, b: a + b) #(user, [tweet_words])
           .flatMapValues(identity) #(user, word)
           .distinct()
           .join(tweet_words) #(user, (word, [tweet_words]))
           .join(union_rows) #(user, ((word, [tweet_words]), {tweet_words_union_set}))
           .reduceByKey(lambda a, b:
                        min(freq(a[0][0], a[0][1]), freq(a[0][0], queried_user_words))
                        + min(freq(b[0][0], b[0][1]), freq(b[0][0], queried2_user_words)))
           .take(k))
           #.coalesce(1)
           #.saveAsTextFile("result/tweets"))

def rmd_usrs2(rdd=TWITTER_RDD, user="tmj_bos_hr", k=3):
    '''Returns list of recommended users'''

    queried_user_tweets = get_user_words(rdd, user) #[queried_user_tweet_words]
    
    return(rdd
           .filter(lambda row: not row[0] == user)
           .mapValues(lambda s: s.split())
           .reduceByKey(lambda a, b: a + b) #(user, [tweet_words])
           .mapValues(lambda l: sim_score(queried_user_tweets, l)) #(user, sim_score)
           .sortByKey()
           .takeOrdered(k, key=lambda t: -t[1]))

def intern_rdd(rdd, user):
    '''Return touple of user and list of tweet words'''
    return(rdd
           .filter(lambda row: not row[0] == user)
           .mapValues(lambda s: s.split()) #(user, [tweet_words])
           .reduceByKey(lambda a, b: a + b)) #(user, [tweet_words]))


def union_rdd(rdd, user):
    '''Return rdd containing touples of user and union words for queried user'''
    user_words = get_user_words(rdd, user)

    return(rdd
           .filter(lambda row: not row[0] == user)
           .mapValues(lambda s: s.split())
           .flatMapValues(identity) #(user, word)
           .distinct()
           .reduceByKey(lambda a, b: a + ' ' + b) #(user, tweet_words_string)
           .mapValues(lambda l: l.split() + user_words)) #(user, [tweet_words_union_set])

def recommended_users(user, k, file_path, output_file):
    '''Writes similar users to file'''
    conf = SparkConf().setAppName("main").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    twitter_headers = ["user", "tweet_text"]
    twitter_rdd = sc.textFile(file_path).map(lambda line: split_and_map(line, twitter_headers))

def main(argv):
    '''Main function'''
    print(argv)
    my_dict = {argv[2]:argv[3], argv[4]:argv[5], argv[6]:argv[7], argv[8]:argv[9]}
    rmd_usrs2(my_dict["-user"], my_dict=["-k"], my_dict["-file"], my_dict["-output"])

if __name__ == "__main__":
    main(sys.argv)
