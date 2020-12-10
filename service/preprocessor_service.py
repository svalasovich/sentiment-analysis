import re
from string import punctuation
from typing import List

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

URL = "[URL]"
AT_USER = "[AT_USER]"

class PreProcessorService:

    def __init__(self):
        self._stopwords = set(stopwords.words('english') + list(punctuation) + [URL, AT_USER])

    def processText(self, text: str) -> List[str]:
        text = text.lower()
        text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', URL, text)
        text = re.sub('@[^\s]+', AT_USER, text)
        text = re.sub(r'#([^\s]+)', r'\1', text)
        text = word_tokenize(text)
        return [word for word in text if word not in self._stopwords]
