'''This module does data aggregation using spark on tweets'''
import sys
from pyspark import SparkConf, SparkContext

def split_to_touple(line):
    '''Splits line into touple'''
    line_list = line.split('\t')
    return (line_list[0], line_list[1])

# Define helper functions for manipulating data and writing to file

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

    with open('{}'.format(filename), 'w') as f:
        f.write('\n'.join(str_list))
    print("File has been written")

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

def get_user_words(rdd, user):
    '''Return list of all distinct user words from tweets'''
    step = rdd.filter(lambda row: row[0] == user)
    if step.count() <= 0:
        raise ValueError("User doesn't exist in file")
    else:
        return(step
               .values()
               .flatMap(lambda val: val.split()) #[user_words]
               .collect())

def rmd_usrs(rdd, user="tmj_bos_hr", k=10):
    '''Returns list of recommended users'''

    queried_user_tweets = get_user_words(rdd, user) #[queried_user_tweet_words]

    result = (rdd
              .filter(lambda row: not row[0] == user)
              .mapValues(lambda s: s.split())
              .reduceByKey(lambda a, b: a + b) #(user, [tweet_words])
              .map(lambda t: (sim_score(queried_user_tweets, t[1]), t[0])) #(sim_score, user)
              .reduceByKey(lambda a, b: a + ' ' + b) #(sim_score, users_string)
              .mapValues(lambda s: sorted(s.split())) #(sim_score, [users])
              .flatMapValues(identity) #(sim_score, user)
              .takeOrdered(k, key=lambda t: -t[0]))

    return map(lambda t: (t[1], t[0]), result)

def rmd_usrs2(rdd, user="tmj_bos_hr", k=10):
    '''Returns list of recommended users'''
    user_words = (rdd
                  .map(lambda x: (x[0], x[1].split(" "))) # split sentence into words
                  .flatMapValues(identity) # flapmap to get user combined with each word
                  .map(lambda x: ((x[0], x[1]), 1)) # assign count to each word
                  .reduceByKey(lambda x, y: x + y)) # aggregate count on each word

    # filter words for main user and others, then make word the key
    count_main = (user_words
                  .filter(lambda x: x[0][0] == user)
                  .map(lambda x: (x[0][1], (x[1], x[0][0]))))
    count_others = (user_words
                    .filter(lambda x: x[0][0] != user)
                    .map(lambda x: (x[0][1], (x[1], x[0][0]))))

    return(count_main
           .join(count_others) # join on common word
           .map(lambda x: (x[1][1][1], min(x[1][0][0], x[1][1][0]))) # find minimum
           .reduceByKey(lambda x, y: x + y).sortByKey(True).sortBy(lambda x: -x[1]) #sum min to get sim score and sort by descending
           .take(k)) # return k top recommended users

def main(argv):
    '''Main function'''
    CONF = SparkConf() \
      .setAppName("main") \
      .setMaster("local[*]") \
      .setExecutorEnv('spark.driver.memory', '5g') \
      .setExecutorEnv('spark.executor.memory', '3g')
    SC = SparkContext(conf=CONF)

    my_dict = {argv[1]:argv[2], argv[3]:argv[4], argv[5]:argv[6], argv[7]:argv[8]}

    my_rdd = SC.textFile(my_dict["-file"]).map(split_to_touple)

    result = rmd_usrs2(rdd=my_rdd, user=my_dict["-user"], k=int(my_dict["-k"]))

    print(result)

    write_result_to_file(result, my_dict["-output"])

if __name__ == "__main__":
    main(sys.argv)
