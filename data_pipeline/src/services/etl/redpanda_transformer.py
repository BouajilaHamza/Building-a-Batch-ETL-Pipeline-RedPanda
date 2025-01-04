import re

import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

from data_pipeline.src.core.logging_config import setup_logging
from data_pipeline.src.services.etl.redpanda_init import RedpandaBase

logger = setup_logging("RedpandaStreamApp")


class RedpandaStreamApp(RedpandaBase):
    def __init__(self):
        super().__init__()
        self.logger = setup_logging("RedpandaStreamApp")
        self.news_sdf = self.app.dataframe(self.news_input_topic)
        self.news_sdf = self.news_sdf.apply(self.transform_news)
        self.news_sdf.to_topic(self.news_output_topic)

        self.bitcoin_sdf = self.app.dataframe(self.bitcoin_input_topic)
        self.bitcoin_sdf = self.bitcoin_sdf.apply(self.transform_bitcoin)
        self.bitcoin_sdf.to_topic(self.bitcoin_output_topic)
        self.stop_words = set(stopwords.words("english"))
        self.lemmatizer = WordNetLemmatizer()
        # self.stemmer = PorterStemmer()

    def transform_news(self, msg: list[dict[str, str]]):
        msg = msg["results"]
        cleaned_msg = []
        for doc in msg:
            clean_doc = {}
            for key in doc:
                if key in ["title", "description"]:
                    # Remove punctuation and numbers
                    doc[key] = re.sub(r"[^\w\s]", "", doc[key])
                    doc[key] = re.sub(r"\d+", "", doc[key])
                    # Convert to lowercase
                    doc[key] = doc[key].lower()
                    # Remove stop words

                    words = doc[key].split()
                    filtered_words = [
                        word for word in words if word not in self.stop_words
                    ]
                    # Tokenize
                    tokens = word_tokenize(" ".join(filtered_words))
                    # Stemming and Lemmatization
                    # stemmed_words = [self.stemmer.stem(word) for word in tokens]
                    lemmatized_words = [
                        self.lemmatizer.lemmatize(word) for word in tokens
                    ]
                    # Remove extra whitespace
                    doc[key] = " ".join(lemmatized_words)
            clean_doc = {
                "title": doc["title"],
                "description": doc["description"],
                "pubDate": doc["pubDate"],
                "source": doc["source_name"],
            }
            self.logger.info(f"Transformed News data: {clean_doc}")
            cleaned_msg.append(clean_doc)

        return cleaned_msg

    def transform_bitcoin(self, msg):
        new_msg = []
        if msg:
            if "data" in msg:
                for i in msg["data"]:
                    new_msg.append(i["quote"]["USD"])
                    self.logger.info(f"Transformed data: {i}")
            # self.logger.info(f"Transformed data: {new_msg}")
            return new_msg

    def run(self):
        self.app.run()


def download_nltk_data():
    nltk.download("punkt_tab")
    nltk.download("wordnet")
    nltk.download("stopwords")


if __name__ == "__main__":
    logger.info("Downloading NLTK data")
    download_nltk_data()
    logger.info("Starting Redpanda Stream App")
    stream_app = RedpandaStreamApp()
    logger.info("Running Redpanda Stream App")
    stream_app.run()
