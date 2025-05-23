# # import socket
# # import pickle
# # import time
# # from message import Message

# # class Producer:
# #     def __init__(self, producer_id, host='192.168.151.51', port=12345):
# #         self.producer_id = producer_id
# #         self.host = host
# #         self.port = port

# #     def send_message(self, msg):
# #         try:
# #             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
# #                 s.connect((self.host, self.port))
# #                 s.sendall(pickle.dumps(msg))
# #                 print(f"Producer {self.producer_id} Sent: {msg.content} (Seq: {msg.sequence})")
# #         except Exception as e:
# #             print(f"Producer {self.producer_id} error: {e}")

# # if __name__ == "__main__":
# #     producer = Producer(producer_id="P2")
# #     messages = [
# #         Message("P2", "Order456", "msg4", 1, "Mua hàng Order456"),
# #         Message("P2", "Order456", "msg5", 2, "Thanh toán Order456")
# #     ]
# #     for msg in messages:
# #         producer.send_message(msg)
# #         time.sleep(1)

# # =====================================================================

# import socket
# import pickle
# import time
# import threading
# from concurrent.futures import ThreadPoolExecutor
# from message import Message

# class Producer:
#     def __init__(self, producer_id, host='192.168.151.51', port=12345, confirm_port=12349):
#         self.producer_id = producer_id
#         self.host = host
#         self.port = port
#         self.confirm_port = confirm_port
#         self.executor = ThreadPoolExecutor(max_workers=10)  # Thêm ThreadPoolExecutor
#         threading.Thread(target=self.start_confirm_server, daemon=True).start()

#     def send_message(self, msg):
#         try:
#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                 s.connect((self.host, self.port))
#                 s.sendall(pickle.dumps(msg))
#                 print(f"Producer {self.producer_id} Sent: {msg.content} (Seq: {msg.sequence})")
#         except Exception as e:
#             print(f"Producer {self.producer_id} error: {msg.content} - {e}")

#     def start_confirm_server(self):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#             try:
#                 server.bind((self.host, self.confirm_port))
#                 server.listen()
#                 print(f"Producer {self.producer_id} Confirm Server running on {self.host}:{self.confirm_port}")
#                 while True:
#                     client_socket, addr = server.accept()
#                     self.executor.submit(self.handle_confirm, client_socket, addr)
#             except Exception as e:
#                 print(f"Producer {self.producer_id} Confirm Server failed: {e}")

#     def handle_confirm(self, client_socket, addr):
#         try:
#             with client_socket:
#                 client_socket.settimeout(5)  # Thêm timeout
#                 data = client_socket.recv(1024 * 1024)
#                 if data:
#                     confirm_msg = pickle.loads(data)
#                     print(f"Producer {self.producer_id} Received Confirm: {confirm_msg.content} (Seq: {confirm_msg.sequence}, From: {confirm_msg.producer_id})")
#                 else:
#                     print(f"Producer {self.producer_id} Confirm disconnected from {addr}")
#         except Exception as e:
#             print(f"Producer {self.producer_id} Confirm error from {addr}: {e}")

# if __name__ == "__main__":
#     producer = Producer(producer_id="P2")
#     messages = [
#         Message("P2", "Order456", "msg4", 1, "Mua hàng Order456"),
#         Message("P2", "Order456", "msg5", 2, "Thanh toán Order456")
#     ]
#     for msg in messages:
#         producer.send_message(msg)
#         time.sleep(1)

import socket
import pickle
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from message import Message

class Producer:
    def __init__(self, producer_id, host='192.168.107.51', port=12345, confirm_port=12349):
        self.producer_id = producer_id
        self.host = host
        self.port = port
        self.confirm_port = confirm_port
        self.executor = ThreadPoolExecutor(max_workers=10)
        threading.Thread(target=self.start_confirm_server, daemon=True).start()
        time.sleep(2)  # Đợi server confirm khởi động

    def send_message(self, msg):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024 * 10)
                s.connect((self.host, self.port))
                s.sendall(pickle.dumps(msg))
                print(f"Producer {self.producer_id} Sent: {msg.content} (Seq: {msg.sequence})")
        except Exception as e:
            print(f"Producer {self.producer_id} error: {msg.content} - {e}")

    def start_confirm_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                server.bind((self.host, self.confirm_port))
                server.listen()
                print(f"Producer {self.producer_id} Confirm Server running on {self.host}:{self.confirm_port}")
                while True:
                    client_socket, addr = server.accept()
                    self.executor.submit(self.handle_confirm, client_socket, addr)
            except Exception as e:
                print(f"Producer {self.producer_id} Confirm Server failed: {e}")

    def handle_confirm(self, client_socket, addr):
        try:
            with client_socket:
                client_socket.settimeout(15)  # Tăng timeout
                print(f"Producer {self.producer_id} handling confirm from {addr}")  # Debug
                data = client_socket.recv(1024 * 1024)
                if data:
                    confirm_msg = pickle.loads(data)
                    print(f"Producer {self.producer_id} Received Confirm: {confirm_msg.content} (Seq: {confirm_msg.sequence}, From: {confirm_msg.producer_id})")
                else:
                    print(f"Producer {self.producer_id} Confirm disconnected from {addr}")
        except Exception as e:
            print(f"Producer {self.producer_id} Confirm error from {addr}: {e}")

if __name__ == "__main__":
    producer = Producer(producer_id="P2")
    messages = [
        Message("P2", "Order456", "msg4", 1, "Mua hàng Order456"),
        Message("P2", "Order456", "msg5", 2, "Thanh toán Order456")
    ]
    for msg in messages:
        producer.send_message(msg)
        time.sleep(1)