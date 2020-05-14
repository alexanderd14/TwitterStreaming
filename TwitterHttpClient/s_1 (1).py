#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 2185
dataStream = ssc.socketTextStream("localhost",2243)

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# In[ ]:


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']
def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))
        # create a DF from the Row RDD
        words_df = sql_context.createDataFrame(row_rdd)
        # Register the dataframe as table
        words_df.registerTempTable("words")
        # get the top 10 hashtags from the table using SQL and print them
        word_count_df = sql_context.sql("select word, word_count from words order by word_count desc limit 10")
        word_count_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(word_count_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)



# In[ ]:


# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '' in w).map(lambda x: (x, 1))

# adding the count of each hashtag to its last count using updateStateByKey
tags_totals = hashtags.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), 600, 30)
tags_totals.pprint()

# do the processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()

# In[ ]:



