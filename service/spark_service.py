from pyspark import Row, RDD
from pyspark.sql import SparkSession, DataFrame

from service import twitter_service
from service.preprocessor_service import PreProcessorService


def classify_tweets(sentiment_analysis_model, number_tweets, search_keywords, path):
    spark = SparkSession.builder.appName('SparkByExamples.com').master("local[*]").getOrCreate()
    twitter = twitter_service.TwitterService(filename="credentials_twitter.json")

    broadcast_model, broadcast_twitter_credentials, broadcast_number_tweets = __prepare_broadcast(spark,
                                                                                                  twitter.credentials,
                                                                                                  number_tweets,
                                                                                                  sentiment_analysis_model)

    tweets_rdd = __prepare_tweets_rdd(spark, search_keywords, broadcast_twitter_credentials, broadcast_number_tweets)
    predicted_tweets = __classify_tweets(tweets_rdd, broadcast_model)
    predicted_tweets.write.orc(path, mode="append")


def __prepare_broadcast(spark, credentials, number_tweets, sentiment_analysis_model):
    broadcast_model = spark.sparkContext.broadcast(sentiment_analysis_model)
    broadcast_twitter_credentials = spark.sparkContext.broadcast(credentials)
    broadcast_number_tweets = spark.sparkContext.broadcast(number_tweets)
    return broadcast_model, broadcast_twitter_credentials, broadcast_number_tweets


def __prepare_tweets_rdd(spark, search_keywords, broadcast_twitter_credentials, broadcast_number_tweets):
    rdd: RDD = spark.sparkContext.parallelize(search_keywords)
    return rdd.flatMap(lambda keyword: twitter_service.TwitterService(
        broadcast_twitter_credentials.value["consumer_key"],
        broadcast_twitter_credentials.value["consumer_secret"],
        broadcast_twitter_credentials.value["access_token_key"],
        broadcast_twitter_credentials.value["access_token_secret"]).search(
        keyword, broadcast_number_tweets.value))


def __classify_tweets(tweets_rdd, broadcast_model) -> DataFrame:
    preprocessed_tweets_rdd = tweets_rdd.map(
        lambda tweet: ({"text": PreProcessorService().processText(tweet["text"]), "label": None}, tweet))

    return preprocessed_tweets_rdd.map(lambda tweet: Row(
        **{"label": str(broadcast_model.value.classify(tweet[0])), "text": tweet[1]["text"],
           "keyword": tweet[1]["keyword"]})).toDF()
