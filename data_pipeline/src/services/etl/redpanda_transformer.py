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

    def clean_text(self, text: str) -> str:
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\d+", "", text)
        text = text.lower()
        words = text.split()
        filtered_words = [word for word in words if word not in self.stop_words]
        tokens = word_tokenize(" ".join(filtered_words))
        lemmatized_words = [self.lemmatizer.lemmatize(word) for word in tokens]
        return " ".join(lemmatized_words)

    def transform_news(self, msg: dict) -> list:
        cleaned_msg = []
        if msg and "results" in msg:
            for doc in msg["results"]:
                try:
                    clean_doc = {
                        "title": self.clean_text(doc.get("title", "")),
                        "description": self.clean_text(doc.get("description", "")),
                        "pubDate": doc.get("pubDate", ""),
                        "source": doc.get("source_name", ""),
                    }
                    self.logger.info(f"Transformed News data: {clean_doc}")
                    cleaned_msg.append(clean_doc)
                except Exception as e:
                    self.logger.error(f"Error transforming news document: {e}")
                    raise Exception(f"Error transforming news document: {e}")
        return cleaned_msg

    def transform_bitcoin(self, msg):
        new_msg = []
        if msg:
            if "data" in msg:
                for i in msg["data"]:
                    i["quote"]["USD"].update({"id": i["id"]})
                    new_msg.append(i["quote"]["USD"])
                    self.logger.debug(f"Transformed data: {i}")
            self.logger.debug(f"Transformed data: {new_msg}")
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
    logger.info("Initializing Topics")
    stream_app.clear_topics()
    logger.info("Running Redpanda Stream App")
    stream_app.run()
