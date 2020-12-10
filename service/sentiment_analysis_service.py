import logging
from typing import List, Any

import nltk

from service import data_service, preprocessor_service

logging.basicConfig(level=logging.INFO)

class SentimentAnalysisService:

    def __init__(self) -> None:
        nltk.download("stopwords")
        self.__logger = logging.getLogger('sentiment_analysis_service.SentimentAnalysisService')
        self.__file_service = data_service.FileService()
        self.__pickle_service = data_service.PickleService()
        self.__preprocessor = preprocessor_service.PreProcessorService()

    def build_vocabulary(self, data: List[List[Any]]):
        self.__logger.info("Started building vocabulary")
        words = []
        for text, label in data:
            words.extend(self.__preprocessor.processText(text))

        wordlist = nltk.FreqDist(words)
        word_features = wordlist.keys()

        self.__word_features = word_features
        self.__logger.info("Finished building vocabulary")

    def train(self, training_data):
        if self.__word_features is None:
            raise RuntimeWarning("SentimentAnalysisService doen't have built vocabulary")

        self.__logger.info("Started training")
        training_features = nltk.classify.apply_features(self.__extract_features, training_data)
        self.__naive_bayes_classifier = nltk.NaiveBayesClassifier.train(training_features)
        self.__logger.info("Finished training")

    def __extract_features(self, text):
        tweet_words = set(text)
        features = {}
        for word in self.__word_features:
            features['contains(%s)' % word] = (word in tweet_words)

        return features

    def prepare_model(self, path_vocabulary = "training_dataset.csv", model_path="model.pickle"):
        try:
            self.__naive_bayes_classifier = self.__pickle_service.read_pickle(model_path)
            self.__logger.info(f"Model loaded from {model_path}")
        except Exception as exc:
            data = self.__file_service.read_csv_file(path_vocabulary)
            data = list(map(lambda row: [row[-1], int(row[0])], data))
            self.build_vocabulary(data)
            self.train(data)
            self.__pickle_service.save_pickle(model_path, self.__naive_bayes_classifier)
            self.__logger.info(f"Model saved to {model_path}")

    @property
    def model(self):
        return self.__naive_bayes_classifier
