from twitter import *

from service.data_service import JsonService


class TwitterService:

    def __init__(self, consumer_key = None, consumer_secret = None, access_token_key = None, access_token_secret = None, filename = None) -> None:
        if filename:
            json_service = JsonService()
            json_file = json_service.read_json(filename)
            self.__twitter_api: Twitter = Twitter(auth=OAuth(json_file["access_token_key"], json_file["access_token_secret"], json_file["consumer_key"], json_file["consumer_secret"]))
            self.credentials = {"access_token_key": json_file["access_token_key"], "access_token_secret":json_file["access_token_secret"], "consumer_key": json_file["consumer_key"], "consumer_secret": json_file["consumer_secret"]}
        else:
            self.__twitter_api: Twitter = Twitter(auth=OAuth(access_token_key, access_token_secret, consumer_key, consumer_secret))
            self.credentials = {"access_token_key": access_token_key, "access_token_secret": access_token_key, "consumer_key": access_token_key, "consumer_secret": access_token_key}

    def search(self, keyword, total, page_size=100):
        result = []
        try:
            next_max_id = None
            while len(result) < total:
                tweets_fetched = self.__twitter_api.search.tweets(q=keyword, count=page_size, lang="en", max_id=next_max_id)
                next_max_id = self.__get_max_id(tweets_fetched)
                for status in tweets_fetched["statuses"]:
                    if len(result) < total:
                        result.append(status["text"])
                    else:
                        break
            print(f"Fetched {len(result)} tweets for the term {keyword}")
            return result
        except Exception as err:
            print("Raised exception.", err)
            return []

    def __get_max_id(self, tweets_fetched):
        next_results = tweets_fetched["search_metadata"]["next_results"]
        return  next_results.replace("?", "").split("=")[1].split("&")[0]