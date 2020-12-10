import logging

from pyspark import RDD, Row
from pyspark.sql import SparkSession, DataFrame

from service import twitter_service
from service.preprocessor_service import PreProcessorService
from service.sentiment_analysis_service import SentimentAnalysisService


def collect_requirements():
    logging.info("Collect requirements step.")
    search_keywords = []
    while True:
        keyword = input("Enter a search keyword: ")
        if keyword:
            search_keywords.append(keyword)
        else:
            break
    number_tweets = int(input("Enter number of tweet for each keyword: "))
    path = input("Enter path for saving result orc: ")
    return search_keywords, number_tweets, path


def prepare_model():
    logging.info("Prepare model step.")
    sentiment_analysis_service = SentimentAnalysisService()
    sentiment_analysis_service.prepare_model()
    return sentiment_analysis_service.model


def classify_tweets(sentiment_analysis_model, number_tweets, search_keywords, path):
    logging.info("Classify and save to orc step")

    spark = SparkSession.builder.appName('SparkByExamples.com').master("local[*]").getOrCreate()
    twitter = twitter_service.TwitterService(filename="credentials_twitter.json")

    broadcast_model = spark.sparkContext.broadcast(sentiment_analysis_model)
    broadcast_twitter_credentials = spark.sparkContext.broadcast(twitter.credentials)
    broadcast_number_tweets = spark.sparkContext.broadcast(number_tweets)

    rdd: RDD = spark.sparkContext.parallelize(search_keywords)
    tweets_rdd = rdd.flatMap(lambda keyword: twitter_service.TwitterService(
        broadcast_twitter_credentials.value["consumer_key"],
        broadcast_twitter_credentials.value["consumer_secret"],
        broadcast_twitter_credentials.value["access_token_key"],
        broadcast_twitter_credentials.value["access_token_secret"]).search(
        keyword, broadcast_number_tweets.value))

    preprocessed_tweets_rdd = tweets_rdd.map(
        lambda tweet: ({"text": PreProcessorService().processText(tweet["text"]), "label": None}, tweet))

    predicted_tweets: DataFrame = preprocessed_tweets_rdd.map(
        lambda tweet: Row(**{"label": str(broadcast_model.value.classify(tweet[0])), "text": tweet[1]["text"],
                             "keyword": tweet[1]["keyword"]})).toDF()

    predicted_tweets.write.orc(path, mode="append")


if __name__ == '__main__':
    search_keywords, number_tweets, path = collect_requirements()
    sentiment_analysis_model = prepare_model()
    classify_tweets(sentiment_analysis_model, number_tweets, search_keywords, path)
    logging.info("Done.")
