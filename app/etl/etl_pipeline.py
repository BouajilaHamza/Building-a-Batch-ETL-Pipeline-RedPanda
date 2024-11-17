class ETLPipeline:
    def __init__(self, ingestors, producer):
        self.ingestors = ingestors
        self.producer = producer

    def run(self):
        for ingestor in self.ingestors:
            data = ingestor.fetch_data()
            transformed_data = self.transform(data)
            self.producer.produce_data(transformed_data)

    def transform(self, data):
        # Perform data transformation here
        return data
