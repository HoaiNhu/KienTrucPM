# import socket
# import pickle
# from message import Message
# import time

# class Consumer:
#     def __init__(self, partition_ids=[0], host='192.168.151.51', base_port=12346):
#         self.partition_ids = partition_ids
#         self.host = host
#         self.base_port = base_port
#         self.sockets = {}

#     def process(self):
#         while True:
#             try:
#                 # Kết nối tới tất cả partition
#                 for partition_id in self.partition_ids:
#                     port = self.base_port + partition_id
#                     if partition_id not in self.sockets or not self.sockets[partition_id]:
#                         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                         sock.settimeout(5)  # Tăng timeout để chờ message
#                         try:
#                             sock.connect((self.host, port))
#                             self.sockets[partition_id] = sock
#                             print(f"Consumer connected to Partition {partition_id} at {self.host}:{port}")
#                         except Exception as e:
#                             print(f"Failed to connect to Partition {partition_id}: {e}")
#                             self.sockets[partition_id] = None
                
#                 # Nhận message từ các socket
#                 for partition_id, sock in self.sockets.items():
#                     if not sock:
#                         continue
#                     try:
#                         data = sock.recv(10 * 1024 * 1024)
#                         if data:
#                             msg = pickle.loads(data)
#                             process_time = (time.time() - msg.timestamp) * 1000
#                             print(f"Consumer Partition {partition_id} Processed: {msg.content} (Seq: {msg.sequence}, Producer: {msg.producer_id}, Time: {process_time:.2f}ms)")
#                             sock.sendall("ACK".encode())
#                         else:
#                             print(f"Consumer Partition {partition_id} disconnected by Broker")
#                             self.sockets[partition_id] = None
#                     except socket.timeout:
#                         continue
#                     except Exception as e:
#                         print(f"Consumer Partition {partition_id} error: {e}")
#                         self.sockets[partition_id] = None
#             except Exception as e:
#                 print(f"Consumer error: {e}")
#                 time.sleep(1)

# if __name__ == "__main__":
#     consumer = Consumer(partition_ids=[0])
#     consumer.process()

# =================================================
import socket
import pickle
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from message import Message

class Consumer:
    def __init__(self, partition_ids=[0], host='192.168.107.51', base_port=12346):
        self.partition_ids = partition_ids
        self.host = host
        self.base_port = base_port
        self.sockets = {}
        self.executor = ThreadPoolExecutor(max_workers=10)  # Song song hóa

    def process_message(self, partition_id, sock, data):
        try:
            msg = pickle.loads(data)
            process_time = (time.time() - msg.timestamp) * 1000
            print(f"Consumer Partition {partition_id} Processed: {msg.content} (Seq: {msg.sequence}, Producer: {msg.producer_id}, Time: {process_time:.2f}ms)")
            confirm_msg = Message(
                producer_id=f"C{partition_id}",
                order_id=msg.order_id,
                message_id=f"confirm_{msg.message_id}",
                sequence=msg.sequence,
                content=f"Confirm: Processed {msg.content}"
            )
            sock.sendall(pickle.dumps(confirm_msg, protocol=pickle.HIGHEST_PROTOCOL))
        except Exception as e:
            print(f"Consumer Partition {partition_id} process error: {e}")

    def process(self):
        while True:
            try:
                for partition_id in self.partition_ids:
                    port = self.base_port + partition_id
                    if partition_id not in self.sockets or not self.sockets[partition_id]:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(60)  # Tăng timeout
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 20 * 1024 * 1024)  # Tăng buffer
                        retry_count = 5
                        while retry_count > 0:
                            try:
                                sock.connect((self.host, port))
                                self.sockets[partition_id] = sock
                                print(f"Consumer connected to Partition {partition_id} at {self.host}:{port}")
                                break
                            except Exception as e:
                                print(f"Failed to connect to Partition {partition_id}: {e}")
                                retry_count -= 1
                                time.sleep(1)
                        if retry_count == 0:
                            self.sockets[partition_id] = None
                
                for partition_id, sock in self.sockets.items():
                    if not sock:
                        continue
                    try:
                        print(f"Consumer Partition {partition_id} attempting to receive data")
                        data = sock.recv(20 * 1024 * 1024)
                        if data:
                            self.executor.submit(self.process_message, partition_id, sock, data)
                        else:
                            print(f"Consumer Partition {partition_id} disconnected by Broker")
                            self.sockets[partition_id] = None
                    except socket.timeout:
                        print(f"Consumer Partition {partition_id} receive timeout")
                        continue
                    except Exception as e:
                        print(f"Consumer Partition {partition_id} error: {e}")
                        self.sockets[partition_id] = None
            except Exception as e:
                print(f"Consumer error: {e}")
                time.sleep(1)

if __name__ == "__main__":
    consumer = Consumer(partition_ids=[0])
    consumer.process()