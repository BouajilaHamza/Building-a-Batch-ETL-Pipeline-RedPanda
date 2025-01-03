import re

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

    def transform_news(self, msg):
        # Remove punctuation and numbers
        msg["description"] = re.sub(r"[^\w\s]", "", msg["description"])
        msg["description"] = re.sub(r"\d+", "", msg["description"])

        # Convert to lowercase
        msg["description"] = msg["description"].lower()

        # Remove stop words
        stop_words = set(stopwords.words("english"))
        words = msg["description"].split()
        filtered_words = [word for word in words if word not in stop_words]

        # Tokenize
        tokens = word_tokenize(" ".join(filtered_words))

        # Stemming and Lemmatization
        # stemmer = PorterStemmer()
        lemmatizer = WordNetLemmatizer()
        # stemmed_words = [stemmer.stem(word) for word in tokens]
        # print(stemmed_words)
        lemmatized_words = [lemmatizer.lemmatize(word) for word in tokens]

        # Remove extra whitespace
        msg["description"] = " ".join(lemmatized_words)
        clean_msg = {
            "title": msg["title"],
            "description": msg["description"],
            "pubDate": msg["pubDate"],
            "source": msg["source_name"],
        }

        return clean_msg

    def transform_bitcoin(self, msg):
        new_msg = []
        if msg:
            if "data" in msg:
                for i in msg["data"]:
                    new_msg.append(i["quote"]["USD"])
                    self.logger.info(f"Transformed data: {i}")
            self.logger.info(f"Transformed data: {new_msg}")
            return new_msg

    def run(self):
        self.app.run()


if __name__ == "__main__":
    logger.info("Starting Redpanda Stream App")
    stream_app = RedpandaStreamApp()
    stream_app.run()
