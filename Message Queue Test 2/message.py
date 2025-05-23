import time

class Message:
    def __init__(self, producer_id, order_id, message_id, sequence, content):
        self.producer_id = producer_id
        self.order_id = order_id
        self.message_id = message_id
        self.sequence = sequence
        self.content = content
        self.timestamp = time.time()