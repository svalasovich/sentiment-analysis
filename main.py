import logging

from service.sentiment_analysis_service import SentimentAnalysisService
from service.spark_service import classify_tweets


def collect_requirements():
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
    sentiment_analysis_service = SentimentAnalysisService()
    sentiment_analysis_service.prepare_model()
    return sentiment_analysis_service.model


if __name__ == '__main__':
    logging.info("Collect requirements step.")
    search_keywords, number_tweets, path = collect_requirements()
    logging.info("Prepare model step.")
    sentiment_analysis_model = prepare_model()
    logging.info("Classify and save step.")
    classify_tweets(sentiment_analysis_model, number_tweets, search_keywords, path)
    logging.info("Done.")
