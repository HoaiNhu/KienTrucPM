# from broker import MessageBroker
# from producer1 import Producer as Producer1  # Import từ producer1.py
# from producer2 import Producer as Producer2  # Import từ producer2.py
# from consumer1 import Consumer as Consumer1  # Import từ consumer1.py
# from consumer2 import Consumer as Consumer2  # Import từ consumer2.py
# from message import Message
# import threading
# import time

# def run_broker():
#     broker = MessageBroker(host='192.168.151.51')
#     input("Press Enter to exit...\n")

# def run_consumer(consumer_class, partition_ids, name):
#     consumer = consumer_class(partition_ids=partition_ids, host='192.168.151.51')
#     consumer.process()

# def run_producer(producer_class, producer_id, messages):
#     producer = producer_class(producer_id=producer_id, host='192.168.151.51')
#     for msg in messages:
#         producer.send_message(msg)
#         time.sleep(1)  # Tăng delay để Consumer kịp nhận

# if __name__ == "__main__":
#     # Chạy Broker
#     threading.Thread(target=run_broker, daemon=True).start()
#     time.sleep(2)  # Chờ Broker khởi động

#     # Chạy Consumer
#     threading.Thread(target=run_consumer, args=(Consumer1, [0], "Consumer0"), daemon=True).start()
#     threading.Thread(target=run_consumer, args=(Consumer2, [1], "Consumer1"), daemon=True).start()
#     time.sleep(5)  # Chờ Consumer kết nối

#     # Chạy Producer
#     p1_messages = [
#         Message("P1", "Order123", "msg1", 1, "Mua hàng Order123"),
#         Message("P1", "Order123", "msg2", 2, "Thanh toán Order123"),
#         Message("P1", "Order123", "msg3", 3, "Hoàn tất Order123")
#     ]
#     p2_messages = [
#         Message("P2", "Order456", "msg4", 1, "Mua hàng Order456"),
#         Message("P2", "Order456", "msg5", 2, "Thanh toán Order456")
#     ]
#     threading.Thread(target=run_producer, args=(Producer1, "P1", p1_messages), daemon=True).start()
#     threading.Thread(target=run_producer, args=(Producer2, "P2", p2_messages), daemon=True).start()

#     input("Press Enter to exit...\n")
from broker import MessageBroker
from producer1 import Producer as Producer1
from producer2 import Producer as Producer2
from consumer1 import Consumer as Consumer1
from consumer2 import Consumer as Consumer2
from message import Message
import threading
import time

def run_broker():
    broker = MessageBroker(host='0.0.0.0')
    try:
        input("Press Enter to exit...\n")
    except (EOFError, KeyboardInterrupt):
        print("Broker shutting down")

def run_consumer(consumer_class, partition_ids, name):
    consumer = consumer_class(partition_ids=partition_ids, host='192.168.107.51')
    consumer.process()

def run_producer(producer_class, producer_id, messages, confirm_port):
    producer = producer_class(producer_id=producer_id, host='192.168.107.51', confirm_port=confirm_port)
    for msg in messages:
        producer.send_message(msg)
        time.sleep(0.1)

if __name__ == "__main__":
    threading.Thread(target=run_broker, daemon=True).start()
    time.sleep(5)
    threading.Thread(target=run_consumer, args=(Consumer1, [0], "Consumer0"), daemon=True).start()
    threading.Thread(target=run_consumer, args=(Consumer2, [1], "Consumer1"), daemon=True).start()
    time.sleep(10)
    p1_messages = [
        Message("P1", f"Order123_{i}", f"msg{i}", i+1, f"Mua hàng Order123_{i}")
        for i in range(100)
    ]
    p2_messages = [
        Message("P2", f"Order456_{i}", f"msg{i+100}", i+1, f"Mua hàng Order456_{i}")
        for i in range(100)
    ]
    threading.Thread(target=run_producer, args=(Producer1, "P1", p1_messages, 12348), daemon=True).start()
    threading.Thread(target=run_producer, args=(Producer2, "P2", p2_messages, 12349), daemon=True).start()
    try:
        input("Press Enter to exit...\n")
    except (EOFError, KeyboardInterrupt):
        print("Main shutting down")