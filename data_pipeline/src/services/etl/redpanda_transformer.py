from data_pipeline.src.core.logging_config import setup_logging
from data_pipeline.src.services.etl.redpanda_init import RedpandaBase

logger = setup_logging("RedpandaStreamApp")


class RedpandaStreamApp(RedpandaBase):
    def __init__(self):
        super().__init__()
        self.logger = setup_logging("RedpandaStreamApp")
        self.sdf = self.app.dataframe(self.input_topic)
        self.sdf = self.sdf.apply(self.transform)
        self.sdf.to_topic(self.output_topic)

    def transform(self, msg):
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
