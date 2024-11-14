import quixstreams as qx

class RedpandaProducer:
    def __init__(self):
        self.client = qx.create_client()

    def produce_data(self, data):
        stream = self.client.create_stream()
        stream.write(data)
