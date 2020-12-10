from pyspark import RDD, Row

from service import twitter_service, preprocessor_service, data_service
from service.preprocessor_service import PreProcessorService
from service.sentiment_analysis_service import SentimentAnalysisService
import logging
from pyspark.sql import SparkSession, DataFrame


def prepare_model():
    logging.info("Prepare model step")
    file_serv = data_service.FileService()
    data = file_serv.read_csv_file("training_dataset.csv")
    sentiment_analysis_service = SentimentAnalysisService()
    data = list(map(lambda row: [row[-1], int(row[0])], data))
    sentiment_analysis_service.build_vocabulary(data)
    sentiment_analysis_service.train(data)
    return sentiment_analysis_service


def predict_examples(sentiment_analysis_model, number_tweets, search_keywords, path):
    logging.info("Predict and save to database step")

    spark = SparkSession.builder.appName('SparkByExamples.com').master("local[*]").getOrCreate()

    twitter = twitter_service.TwitterService(filename="credentials_twitter.json")
    broadcast_model = spark.sparkContext.broadcast(sentiment_analysis_model)
    broadcast_twitter_credentials = spark.sparkContext.broadcast(twitter.credentials)
    broadcast_number_tweets = spark.sparkContext.broadcast(number_tweets)

    rdd: RDD = spark.sparkContext.parallelize(search_keywords)
    tweets_rdd = rdd.flatMap(lambda keyword: twitter_service.TwitterService(broadcast_twitter_credentials.value["consumer_key"],
                                                                            broadcast_twitter_credentials.value["consumer_secret"],
                                                                            broadcast_twitter_credentials.value["access_token_key"],
                                                                            broadcast_twitter_credentials.value["access_token_secret"])
                             .search(keyword, broadcast_number_tweets.value))
    preprocessed_tweets_rdd = tweets_rdd.map(lambda tweet: (PreProcessorService().processText(tweet), tweet))
    predicted_tweets: DataFrame = preprocessed_tweets_rdd.map(lambda tweet: Row(**{"label": broadcast_model.value.predict(tweet[0]), "text": tweet[1]})).toDF()
    predicted_tweets.write.orc(path, mode="append")


if __name__ == '__main__':
    search_keywords = []
    while True:
        keyword = input("Enter a search keyword: ")
        if keyword:
            search_keywords.append(keyword)
        else:
            break
    number_tweets = int(input("Enter number of tweet for each keyword: "))
    path = input("Enter path for saving result orc: ")
    sentiment_analysis_model = prepare_model()
    predict_examples(sentiment_analysis_model, number_tweets, search_keywords, path)
