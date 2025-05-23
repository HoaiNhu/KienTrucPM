# # import socket
# # import threading
# # import pickle
# # from queue import PriorityQueue
# # from message import Message
# # import time
# # from concurrent.futures import ThreadPoolExecutor

# # class MessageBroker:
# #     def __init__(self, host='0.0.0.0', producer_port=12345, base_consumer_port=12346):
# #         self.partitions = {}  # {partition_id: PriorityQueue}
# #         self.partition_load = {}  # {partition_id: số tin nhắn chờ}
# #         self.consumer_sockets = {}  # {partition_id: list of sockets}
# #         self.producer_partitions = {}  # {producer_id: partition_id}
# #         self.host = host
# #         self.producer_port = producer_port
# #         self.base_consumer_port = base_consumer_port
# #         self.lock = threading.Lock()
# #         self.max_load = 100
# #         self.max_partitions = 10
# #         self.message_log = []  # Lưu trữ tạm để khôi phục
# #         self.executor = ThreadPoolExecutor(max_workers=50)
        
# #         # Khởi tạo 2 partition ban đầu
# #         self.add_partition(0)
# #         self.add_partition(1)
        
# #         threading.Thread(target=self.start_producer_server, daemon=True).start()

# #     def add_partition(self, partition_id):
# #         with self.lock:
# #             if partition_id not in self.partitions:
# #                 self.partitions[partition_id] = PriorityQueue()
# #                 self.partition_load[partition_id] = 0
# #                 self.consumer_sockets[partition_id] = []
# #                 threading.Thread(target=self.start_consumer_server, args=(partition_id,), daemon=True).start()
# #                 print(f"Added Partition {partition_id}")

# #     def start_producer_server(self):
# #         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
# #             server.bind((self.host, self.producer_port))
# #             server.listen()
# #             print(f"Broker Producer Server running on {self.host}:{self.producer_port}")
# #             while True:
# #                 client_socket, addr = server.accept()
# #                 self.executor.submit(self.handle_producer, client_socket, addr)

# #     def handle_producer(self, client_socket, addr):
# #         try:
# #             with client_socket:
# #                 data = client_socket.recv(10 * 1024 * 1024)
# #                 if data:
# #                     msg = pickle.loads(data)
# #                     self.send_to_partition(msg)
# #                     self.message_log.append(msg)
# #                     print(f"Received from {addr}: {msg.content} (Seq: {msg.sequence}, Producer: {msg.producer_id})")
# #                 else:
# #                     print(f"Producer at {addr} disconnected")
# #         except Exception as e:
# #             print(f"Producer error at {addr}: {e}")

# #     def send_to_partition(self, msg):
# #         with self.lock:
# #             # Gán partition cố định để khớp với Consumer
# #             if msg.producer_id not in self.producer_partitions:
# #                 partition_id = 1 if msg.producer_id == "P1" else 0  # P1 → Partition 1, P2 → Partition 0
# #                 self.producer_partitions[msg.producer_id] = partition_id
# #             else:
# #                 partition_id = self.producer_partitions[msg.producer_id]

# #             # Kiểm tra partition quá tải
# #             if self.partition_load.get(partition_id, 0) > self.max_load:
# #                 new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
# #                 self.producer_partitions[msg.producer_id] = new_partition_id
# #                 partition_id = new_partition_id
# #                 print(f"Producer {msg.producer_id} moved to Partition {partition_id} due to load")

# #             # Thêm tin nhắn vào partition
# #             self.partitions[partition_id].put((msg.timestamp, msg.sequence, msg))
# #             self.partition_load[partition_id] = self.partition_load.get(partition_id, 0) + 1
# #             print(f"Added to Partition {partition_id}: {msg.content} (Seq: {msg.sequence})")

# #             # Gửi tới Consumer ngay lập tức
# #             self.send_to_consumer(partition_id, msg)

# #     def send_to_consumer(self, partition_id, msg):
# #         retry_count = 3
# #         while retry_count > 0:
# #             try:
# #                 if not self.consumer_sockets.get(partition_id):
# #                     new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
# #                     self.producer_partitions[msg.producer_id] = new_partition_id
# #                     self.partitions[new_partition_id].put((msg.timestamp, msg.sequence, msg))
# #                     self.partition_load[new_partition_id] = self.partition_load.get(new_partition_id, 0) + 1
# #                     print(f"Partition {partition_id} has no Consumer, reassigned to Partition {new_partition_id}: {msg.content}")
# #                     break
# #                 for sock in self.consumer_sockets[partition_id][:]:  # Copy để tránh lỗi khi sửa list
# #                     sock.sendall(pickle.dumps(msg))
# #                     sock.settimeout(2)  # Tăng timeout
# #                     ack = sock.recv(1024).decode()
# #                     if ack == "ACK":
# #                         self.partition_load[partition_id] -= 1
# #                         print(f"Sent to Consumer {partition_id} and deleted: {msg.content} (Seq: {msg.sequence})")
# #                 break
# #             except Exception as e:
# #                 print(f"Error sending to Consumer {partition_id}: {e}")
# #                 self.consumer_sockets[partition_id] = [s for s in self.consumer_sockets[partition_id] if s]
# #                 if not self.consumer_sockets[partition_id]:
# #                     new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
# #                     self.producer_partitions[msg.producer_id] = new_partition_id
# #                     self.partitions[new_partition_id].put((msg.timestamp, msg.sequence, msg))
# #                     self.partition_load[new_partition_id] = self.partition_load.get(new_partition_id, 0) + 1
# #                     print(f"Partition {partition_id} failed, reassigned to Partition {new_partition_id}: {msg.content}")
# #                     break
# #                 retry_count -= 1
# #                 time.sleep(1)

# #     def get_least_loaded_partition(self, exclude=[]):
# #         with self.lock:
# #             min_load = float('inf')
# #             min_partition = None
# #             for pid, load in self.partition_load.items():
# #                 if pid not in exclude and load < min_load:
# #                     min_load = load
# #                     min_partition = pid
# #             if (min_load > self.max_load * 0.8 or min_partition is None) and len(self.partitions) < self.max_partitions:
# #                 new_pid = max(self.partitions.keys()) + 1 if self.partitions else 0
# #                 self.add_partition(new_pid)
# #                 return new_pid
# #             return min_partition

# #     def start_consumer_server(self, partition_id):
# #         port = self.base_consumer_port + partition_id
# #         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
# #             try:
# #                 server.bind((self.host, port))
# #                 server.listen()
# #                 print(f"Broker Consumer Server for Partition {partition_id} running on {self.host}:{port}")
# #                 while True:
# #                     client_socket, addr = server.accept()
# #                     print(f"Consumer connected to Partition {partition_id} from {addr}")
# #                     with self.lock:
# #                         self.consumer_sockets[partition_id].append(client_socket)
# #                     self.executor.submit(self.handle_consumer, partition_id, client_socket)
# #             except Exception as e:
# #                 print(f"Consumer server for Partition {partition_id} failed: {e}")

# #     def handle_consumer(self, partition_id, client_socket):
# #         while True:
# #             with self.lock:
# #                 if not self.partitions[partition_id].empty() and client_socket in self.consumer_sockets[partition_id]:
# #                     try:
# #                         _, _, msg = self.partitions[partition_id].get()
# #                         self.send_to_consumer(partition_id, msg)
# #                     except Exception as e:
# #                         print(f"Error in handle_consumer {partition_id}: {e}")
# #                         with self.lock:
# #                             self.consumer_sockets[partition_id] = [s for s in self.consumer_sockets[partition_id] if s != client_socket]
# #                         break
# #             time.sleep(0.01)

# # if __name__ == "__main__":
# #     broker = MessageBroker(host='192.168.151.51')
# #     input("Press Enter to exit...\n")

# # ===========================================

# import socket
# import threading
# import pickle
# from queue import PriorityQueue
# from message import Message
# import time
# from concurrent.futures import ThreadPoolExecutor

# class MessageBroker:
#     def __init__(self, host='0.0.0.0', producer_port=12345, base_consumer_port=12346):
#         self.partitions = {}
#         self.partition_load = {}
#         self.consumer_sockets = {}
#         self.producer_partitions = {}
#         self.producer_sockets = {}  # {producer_id: socket}
#         self.host = host
#         self.producer_port = producer_port
#         self.base_consumer_port = base_consumer_port
#         self.lock = threading.Lock()
#         self.max_load = 100
#         self.max_partitions = 10
#         self.message_log = []
#         self.executor = ThreadPoolExecutor(max_workers=50)
        
#         self.add_partition(0)
#         self.add_partition(1)
        
#         threading.Thread(target=self.start_producer_server, daemon=True).start()

#     def add_partition(self, partition_id):
#         with self.lock:
#             if partition_id not in self.partitions:
#                 self.partitions[partition_id] = PriorityQueue()
#                 self.partition_load[partition_id] = 0
#                 self.consumer_sockets[partition_id] = []
#                 threading.Thread(target=self.start_consumer_server, args=(partition_id,), daemon=True).start()
#                 print(f"Added Partition {partition_id}")

#     def start_producer_server(self):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#             server.bind((self.host, self.producer_port))
#             server.listen()
#             print(f"Broker Producer Server running on {self.host}:{self.producer_port}")
#             while True:
#                 client_socket, addr = server.accept()
#                 self.executor.submit(self.handle_producer, client_socket, addr)

#     def handle_producer(self, client_socket, addr):
#         try:
#             with client_socket:
#                 data = client_socket.recv(10 * 1024 * 1024)
#                 if data:
#                     msg = pickle.loads(data)
#                     # Tạo socket mới cho Producer khi cần
#                     with self.lock:
#                         if msg.producer_id not in self.producer_sockets:
#                             sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                             sock.settimeout(5)
#                             retry_count = 3
#                             while retry_count > 0:
#                                 try:
#                                     sock.connect((self.host, 12348 if msg.producer_id == "P1" else 12349))
#                                     self.producer_sockets[msg.producer_id] = sock
#                                     print(f"Connected to Producer {msg.producer_id} confirm server")
#                                     break
#                                 except Exception as e:
#                                     print(f"Failed to connect to Producer {msg.producer_id} confirm server: {e}")
#                                     retry_count -= 1
#                                     time.sleep(1)
#                     self.send_to_partition(msg)
#                     self.message_log.append(msg)
#                     print(f"Received from {addr}: {msg.content} (Seq: {msg.sequence}, Producer: {msg.producer_id})")
#                 else:
#                     print(f"Producer at {addr} disconnected")
#         except Exception as e:
#             print(f"Producer error at {addr}: {e}")
#             with self.lock:
#                 if msg.producer_id in self.producer_sockets:
#                     self.producer_sockets[msg.producer_id].close()
#                     del self.producer_sockets[msg.producer_id]

#     def send_to_partition(self, msg):
#         with self.lock:
#             if msg.producer_id not in self.producer_partitions:
#                 partition_id = 1 if msg.producer_id == "P1" else 0
#                 self.producer_partitions[msg.producer_id] = partition_id
#             else:
#                 partition_id = self.producer_partitions[msg.producer_id]

#             if self.partition_load.get(partition_id, 0) > self.max_load:
#                 new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
#                 self.producer_partitions[msg.producer_id] = new_partition_id
#                 partition_id = new_partition_id
#                 print(f"Producer {msg.producer_id} moved to Partition {partition_id} due to load")

#             self.partitions[partition_id].put((msg.timestamp, msg.sequence, msg))
#             self.partition_load[partition_id] = self.partition_load.get(partition_id, 0) + 1
#             print(f"Added to Partition {partition_id}: {msg.content} (Seq: {msg.sequence})")

#             self.executor.submit(self.send_to_consumer, partition_id, msg)

#     def send_to_consumer(self, partition_id, msg):
#         retry_count = 3
#         while retry_count > 0:
#             try:
#                 if not self.consumer_sockets.get(partition_id):
#                     new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
#                     self.producer_partitions[msg.producer_id] = new_partition_id
#                     self.partitions[new_partition_id].put((msg.timestamp, msg.sequence, msg))
#                     self.partition_load[new_partition_id] = self.partition_load.get(new_partition_id, 0) + 1
#                     print(f"Partition {partition_id} has no Consumer, reassigned to Partition {new_partition_id}: {msg.content}")
#                     break
#                 for sock in self.consumer_sockets[partition_id][:]:
#                     sock.sendall(pickle.dumps(msg))
#                     sock.settimeout(5)
#                     confirm_data = sock.recv(1024 * 1024)
#                     if confirm_data:
#                         confirm_msg = pickle.loads(confirm_data)
#                         if confirm_msg.producer_id.startswith("C"):
#                             self.partition_load[partition_id] -= 1
#                             print(f"Sent to Consumer {partition_id} and received confirm: {confirm_msg.content}")
#                             # Chuyển confirm đến Producer
#                             with self.lock:
#                                 if msg.producer_id in self.producer_sockets:
#                                     try:
#                                         self.producer_sockets[msg.producer_id].sendall(pickle.dumps(confirm_msg))
#                                         print(f"Forwarded confirm to Producer {msg.producer_id}: {confirm_msg.content}")
#                                     except Exception as e:
#                                         print(f"Error sending confirm to Producer {msg.producer_id}: {e}")
#                                         self.producer_sockets[msg.producer_id].close()
#                                         del self.producer_sockets[msg.producer_id]
#                 break
#             except Exception as e:
#                 print(f"Error sending to Consumer {partition_id}: {e}")
#                 self.consumer_sockets[partition_id] = [s for s in self.consumer_sockets[partition_id] if s]
#                 if not self.consumer_sockets[partition_id]:
#                     new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
#                     self.producer_partitions[msg.producer_id] = new_partition_id
#                     self.partitions[new_partition_id].put((msg.timestamp, msg.sequence, msg))
#                     self.partition_load[new_partition_id] = self.partition_load.get(new_partition_id, 0) + 1
#                     print(f"Partition {partition_id} failed, reassigned to Partition {new_partition_id}: {msg.content}")
#                     break
#                 retry_count -= 1
#                 time.sleep(1)

#     def get_least_loaded_partition(self, exclude=[]):
#         with self.lock:
#             min_load = float('inf')
#             min_partition = None
#             for pid, load in self.partition_load.items():
#                 if pid not in exclude and load < min_load:
#                     min_load = load
#                     min_partition = pid
#             if (min_load > self.max_load * 0.8 or min_partition is None) and len(self.partitions) < self.max_partitions:
#                 new_pid = max(self.partitions.keys()) + 1 if self.partitions else 0
#                 self.add_partition(new_pid)
#                 return new_pid
#             return min_partition

#     def start_consumer_server(self, partition_id):
#         port = self.base_consumer_port + partition_id
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             try:
#                 server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#                 server.bind((self.host, port))
#                 server.listen()
#                 print(f"Broker Consumer Server for Partition {partition_id} running on {self.host}:{port}")
#                 while True:
#                     client_socket, addr = server.accept()
#                     print(f"Consumer connected to Partition {partition_id} from {addr}")
#                     with self.lock:
#                         self.consumer_sockets[partition_id].append(client_socket)
#                     self.executor.submit(self.handle_consumer, partition_id, client_socket)
#             except Exception as e:
#                 print(f"Consumer server for Partition {partition_id} failed: {e}")

#     def handle_consumer(self, partition_id, client_socket):
#         while True:
#             with self.lock:
#                 if not self.partitions[partition_id].empty() and client_socket in self.consumer_sockets[partition_id]:
#                     try:
#                         _, _, msg = self.partitions[partition_id].get()
#                         self.send_to_consumer(partition_id, msg)
#                     except Exception as e:
#                         print(f"Error in handle_consumer {partition_id}: {e}")
#                         with self.lock:
#                             self.consumer_sockets[partition_id] = [s for s in self.consumer_sockets[partition_id] if s != client_socket]
#                         break
#             time.sleep(0.01)

# if __name__ == "__main__":
#     broker = MessageBroker(host='192.168.151.51')
#     input("Press Enter to exit...\n")

import socket
import threading
import pickle
from queue import PriorityQueue
from message import Message
import time
from concurrent.futures import ThreadPoolExecutor

class MessageBroker:
    def __init__(self, host='0.0.0.0', producer_port=12345, base_consumer_port=12346):
        self.partitions = {}
        self.partition_load = {}
        self.consumer_sockets = {}
        self.producer_partitions = {}
        self.producer_sockets = {}
        self.host = host
        self.producer_port = producer_port
        self.base_consumer_port = base_consumer_port
        self.lock = threading.Lock()
        self.max_load = 100
        self.max_partitions = 10
        self.message_log = []
        self.executor = ThreadPoolExecutor(max_workers=50)
        
        self.add_partition(0)
        self.add_partition(1)
        
        threading.Thread(target=self.start_producer_server, daemon=True).start()

    def add_partition(self, partition_id):
        with self.lock:
            if partition_id not in self.partitions:
                self.partitions[partition_id] = PriorityQueue()
                self.partition_load[partition_id] = 0
                self.consumer_sockets[partition_id] = []
                threading.Thread(target=self.start_consumer_server, args=(partition_id,), daemon=True).start()
                print(f"Added Partition {partition_id}")

    def start_producer_server(self):
        retry_count = 5
        while retry_count > 0:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server.bind((self.host, self.producer_port))
                    server.listen()
                    print(f"Broker Producer Server running on {self.host}:{self.producer_port}")
                    while True:
                        client_socket, addr = server.accept()
                        self.executor.submit(self.handle_producer, client_socket, addr)
            except Exception as e:
                print(f"Broker Producer Server failed: {e}")
                retry_count -= 1
                time.sleep(2)
            if retry_count == 0:
                print(f"Broker Producer Server failed after retries")

    def handle_producer(self, client_socket, addr):
        try:
            with client_socket:
                data = client_socket.recv(10 * 1024 * 1024)
                if data:
                    msg = pickle.loads(data)
                    with self.lock:
                        if msg.producer_id not in self.producer_sockets:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(30)
                            retry_count = 5
                            while retry_count > 0:
                                try:
                                    sock.connect(('192.168.107.51', 12348 if msg.producer_id == "P1" else 12349))
                                    self.producer_sockets[msg.producer_id] = sock
                                    print(f"Connected to Producer {msg.producer_id} confirm server")
                                    break
                                except Exception as e:
                                    print(f"Failed to connect to Producer {msg.producer_id} confirm server: {e}")
                                    retry_count -= 1
                                    time.sleep(2)
                    self.send_to_partition(msg)
                    with self.lock:
                        self.message_log.append(msg)
                        if len(self.message_log) > 1000:
                            self.message_log = self.message_log[-1000:]
                        print(f"Message log size: {len(self.message_log)}")
                    print(f"Received from {addr}: {msg.content} (Seq: {msg.sequence}, Producer: {msg.producer_id})")
                else:
                    print(f"Producer at {addr} disconnected")
        except Exception as e:
            print(f"Producer error at {addr}: {e}")
            with self.lock:
                if msg.producer_id in self.producer_sockets:
                    self.producer_sockets[msg.producer_id].close()
                    del self.producer_sockets[msg.producer_id]
                    print(f"Closed Producer {msg.producer_id} socket")

    def send_to_partition(self, msg):
        with self.lock:
            if msg.producer_id not in self.producer_partitions:
                partition_id = 1 if msg.producer_id == "P1" else 0
                self.producer_partitions[msg.producer_id] = partition_id
            else:
                partition_id = self.producer_partitions[msg.producer_id]

            if self.partition_load.get(partition_id, 0) > self.max_load:
                new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
                self.producer_partitions[msg.producer_id] = new_partition_id
                partition_id = new_partition_id
                print(f"Producer {msg.producer_id} moved to Partition {partition_id} due to load")

            self.partitions[partition_id].put((msg.timestamp, msg.sequence, msg))
            self.partition_load[partition_id] = self.partition_load.get(partition_id, 0) + 1
            print(f"Added to Partition {partition_id}: {msg.content} (Seq: {msg.sequence})")

            self.executor.submit(self.send_to_consumer, partition_id, msg)

    def send_to_consumer(self, partition_id, msg):
        retry_count = 3
        while retry_count > 0:
            try:
                if not self.consumer_sockets.get(partition_id):
                    new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
                    self.producer_partitions[msg.producer_id] = new_partition_id
                    self.partitions[new_partition_id].put((msg.timestamp, msg.sequence, msg))
                    self.partition_load[new_partition_id] = self.partition_load.get(new_partition_id, 0) + 1
                    print(f"Partition {partition_id} has no Consumer, reassigned to Partition {new_partition_id}: {msg.content}")
                    break
                # Chỉ gửi đến socket Consumer đầu tiên
                sock = self.consumer_sockets[partition_id][0] if self.consumer_sockets[partition_id] else None
                if sock:
                    print(f"Sending to Consumer {partition_id}: {msg.content} (Seq: {msg.sequence})")
                    sock.sendall(pickle.dumps(msg))
                    sock.settimeout(30)
                    confirm_data = sock.recv(1024 * 1024)
                    if confirm_data:
                        confirm_msg = pickle.loads(confirm_data)
                        if confirm_msg.producer_id.startswith("C"):
                            self.partition_load[partition_id] -= 1
                            print(f"Sent to Consumer {partition_id} and received confirm: {confirm_msg.content}")
                            with self.lock:
                                if msg.producer_id in self.producer_sockets:
                                    confirm_retry = 5
                                    while confirm_retry > 0:
                                        try:
                                            self.producer_sockets[msg.producer_id].sendall(pickle.dumps(confirm_msg))
                                            print(f"Forwarded confirm to Producer {msg.producer_id}: {confirm_msg.content}")
                                            break
                                        except Exception as e:
                                            print(f"Retry {confirm_retry} failed sending confirm to Producer {msg.producer_id}: {e}")
                                            confirm_retry -= 1
                                            time.sleep(1)
                                    if confirm_retry == 0:
                                        print(f"Failed to send confirm to Producer {msg.producer_id} after retries")
                                        self.producer_sockets[msg.producer_id].close()
                                        del self.producer_sockets[msg.producer_id]
                                        print(f"Closed Producer {msg.producer_id} socket")
                break
            except Exception as e:
                print(f"Error sending to Consumer {partition_id}: {e}")
                self.consumer_sockets[partition_id] = [s for s in self.consumer_sockets[partition_id] if s]
                if not self.consumer_sockets[partition_id]:
                    new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
                    self.producer_partitions[msg.producer_id] = new_partition_id
                    self.partitions[new_partition_id].put((msg.timestamp, msg.sequence, msg))
                    self.partition_load[new_partition_id] = self.partition_load.get(new_partition_id, 0) + 1
                    print(f"Partition {partition_id} failed, reassigned to Partition {new_partition_id}: {msg.content}")
                    break
                retry_count -= 1
                time.sleep(2)

    def get_least_loaded_partition(self, exclude=[]):
        with self.lock:
            min_load = float('inf')
            min_partition = None
            for pid, load in self.partition_load.items():
                if pid not in exclude and load < min_load:
                    min_load = load
                    min_partition = pid
            if (min_load > self.max_load * 0.8 or min_partition is None) and len(self.partitions) < self.max_partitions:
                new_pid = max(self.partitions.keys()) + 1 if self.partitions else 0
                self.add_partition(new_pid)
                return new_pid
            return min_partition

    def start_consumer_server(self, partition_id):
        port = self.base_consumer_port + partition_id
        retry_count = 5
        while retry_count > 0:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
                    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server.bind((self.host, port))
                    server.listen()
                    print(f"Broker Consumer Server for Partition {partition_id} running on {self.host}:{port}")
                    while True:
                        client_socket, addr = server.accept()
                        print(f"Consumer connected to Partition {partition_id} from {addr}")
                        with self.lock:
                            self.consumer_sockets[partition_id].append(client_socket)
                        self.executor.submit(self.handle_consumer, partition_id, client_socket)
            except Exception as e:
                print(f"Consumer server for Partition {partition_id} failed: {e}")
                retry_count -= 1
                time.sleep(2)
            if retry_count == 0:
                print(f"Consumer server for Partition {partition_id} failed after retries")

    def handle_consumer(self, partition_id, client_socket):
        try:
            while True:
                with self.lock:
                    if not self.partitions[partition_id].empty() and client_socket in self.consumer_sockets[partition_id]:
                        try:
                            _, _, msg = self.partitions[partition_id].get_nowait()
                            self.executor.submit(self.send_to_consumer, partition_id, msg)
                        except Exception as e:
                            print(f"Error in handle_consumer {partition_id}: {e}")
                            break
                time.sleep(0.01)
        except Exception as e:
            print(f"Error in handle_consumer {partition_id}: {e}")
            with self.lock:
                self.consumer_sockets[partition_id] = [s for s in self.consumer_sockets[partition_id] if s != client_socket]

if __name__ == "__main__":
    broker = MessageBroker(host='0.0.0.0')
    try:
        input("Press Enter to exit...\n")
    except (EOFError, KeyboardInterrupt):
        print("Broker shutting down")