# import socket
# import threading
# import pickle
# from queue import PriorityQueue
# from message import Message

# class MessageBroker:
#     def __init__(self, host='0.0.0.0', producer_port=12345, consumer_ports=[12346, 12347]):
#         self.partitions = {0: PriorityQueue(), 1: PriorityQueue()}  # 2 partitions
#         self.host = host
#         self.producer_port = producer_port
#         self.consumer_ports = consumer_ports
#         self.consumer_sockets = {0: None, 1: None}
#         self.max_queue_size = 5  # Ngưỡng "bận"
#         threading.Thread(target=self.start_producer_server, daemon=True).start()
#         for pid in [0, 1]:
#             threading.Thread(target=self.start_consumer_server, args=(pid,), daemon=True).start()

#     def start_producer_server(self):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             server.bind((self.host, self.producer_port))
#             server.listen()
#             print(f"Broker Producer Server running on {self.host}:{self.producer_port}")
#             while True:
#                 client_socket, addr = server.accept()
#                 threading.Thread(target=self.handle_producer, args=(client_socket, addr), daemon=True).start()

#     def handle_producer(self, client_socket, addr):
#         with client_socket:
#             try:
#                 data = client_socket.recv(4096)
#                 if not data:
#                     print(f"No data from {addr}")
#                     return
#                 msg = pickle.loads(data)
#                 self.send_to_partition(msg)
#                 print(f"Received from {addr}: {msg.content} (Seq: {msg.sequence}, Producer: {msg.producer_id})")
#             except Exception as e:
#                 print(f"Error handling producer {addr}: {e}")

#     def send_to_partition(self, msg):
#         # Sharding dựa trên producer_id
#         preferred_partition = int(msg.producer_id) % 2  # Producer 1 -> 0, Producer 2 -> 1
#         # Kiểm tra partition "bận"
#         if self.partitions[preferred_partition].qsize() >= self.max_queue_size:
#             # Chuyển sang partition khác
#             other_partition = 1 - preferred_partition
#             self.partitions[other_partition].put((msg.sequence, msg))
#             print(f"Partition {preferred_partition} busy, added to Partition {other_partition}: {msg.content}")
#         else:
#             self.partitions[preferred_partition].put((msg.sequence, msg))
#             print(f"Added to Partition {preferred_partition}: {msg.content} (Seq: {msg.sequence})")
        
#         # Gửi tới Consumer
#         partition_id = preferred_partition if self.partitions[preferred_partition].qsize() <= self.max_queue_size else other_partition
#         if self.consumer_sockets[partition_id]:
#             try:
#                 self.consumer_sockets[partition_id].sendall(pickle.dumps(msg))
#             except:
#                 self.consumer_sockets[partition_id] = None

#     def start_consumer_server(self, partition_id):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             server.bind((self.host, self.consumer_ports[partition_id]))
#             server.listen()
#             print(f"Broker Consumer Server for Partition {partition_id} running on {self.host}:{self.consumer_ports[partition_id]}")
#             while True:
#                 client_socket, addr = server.accept()
#                 print(f"Consumer connected to Partition {partition_id} from {addr}")
#                 self.consumer_sockets[partition_id] = client_socket
#                 threading.Thread(target=self.handle_consumer, args=(partition_id, client_socket), daemon=True).start()

#     def handle_consumer(self, partition_id, client_socket):
#         while True:
#             if not self.partitions[partition_id].empty() and self.consumer_sockets[partition_id]:
#                 try:
#                     _, msg = self.partitions[partition_id].get()
#                     client_socket.sendall(pickle.dumps(msg))
#                 except:
#                     self.consumer_sockets[partition_id] = None
#                     break
#             import time
#             time.sleep(0.1)

# if __name__ == "__main__":
#     broker = MessageBroker(host='192.168.151.51')  # IP của bạn
#     input("Press Enter to exit...\n")

#=============================================================================#
import socket
import threading
import pickle
from queue import PriorityQueue
from message import Message
import time

class MessageBroker:
    def __init__(self, host='0.0.0.0', producer_port=12345, base_consumer_port=12346):
        self.partitions = {}
        self.partition_load = {}
        self.consumer_sockets = {}
        self.producer_partitions = {}
        self.host = host
        self.producer_port = producer_port
        self.base_consumer_port = base_consumer_port
        self.lock = threading.Lock()
        self.max_load = 100
        self.consumer_sockets = {}
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((self.host, self.producer_port))
            server.listen()
            print(f"Broker Producer Server running on {self.host}:{self.producer_port}")
            while True:
                client_socket, addr = server.accept()
                threading.Thread(target=self.handle_producer, args=(client_socket, addr), daemon=True).start()

    def handle_producer(self, client_socket, addr):
        with client_socket:
            data = client_socket.recv(4096)
            if data:
                msg = pickle.loads(data)
                self.send_to_partition(msg)
                print(f"Received from {addr}: {msg.content} (Seq: {msg.sequence})")

    def send_to_partition(self, msg):
        with self.lock:
            if msg.producer_id not in self.producer_partitions:
                partition_id = self.get_least_loaded_partition()
                self.producer_partitions[msg.producer_id] = partition_id
            else:
                partition_id = self.producer_partitions[msg.producer_id]

            if self.partition_load[partition_id] > self.max_load:
                new_partition_id = self.get_least_loaded_partition(exclude=[partition_id])
                self.producer_partitions[msg.producer_id] = new_partition_id
                partition_id = new_partition_id
                print(f"Producer {msg.producer_id} moved to Partition {partition_id} due to load")

            self.partitions[partition_id].put((msg.timestamp, msg.sequence, msg))
            self.partition_load[partition_id] += 1
            print(f"Added to Partition {partition_id}: {msg.content} (Seq: {msg.sequence})")

            if self.consumer_sockets[partition_id]:
                self.send_to_consumer(partition_id, msg)

    def send_to_consumer(self, partition_id, msg):
        retry_count = 3
        while retry_count > 0:
            try:
                if self.consumer_sockets[partition_id]:
                    self.consumer_sockets[partition_id].sendall(pickle.dumps(msg))
                    self.partition_load[partition_id] -= 1
                    print(f"Sent to Consumer {partition_id}: {msg.content} (Seq: {msg.sequence})")
                    break
                else:
                    print(f"Consumer {partition_id} not connected, waiting...")
                    time.sleep(1)
            except Exception as e:
                print(f"Error sending to Consumer {partition_id}: {e}")
                self.consumer_sockets[partition_id] = None
                retry_count -= 1
                time.sleep(1)  # Chờ trước khi retry
                if retry_count > 0:
                    print(f"Retrying {retry_count} times for Consumer {partition_id}...")

    def get_least_loaded_partition(self, exclude=[]):
        min_load = float('inf')
        min_partition = None
        for pid, load in self.partition_load.items():
            if pid not in exclude and load < min_load:
                min_load = load
                min_partition = pid
        if min_load > self.max_load * 0.8:
            new_pid = max(self.partitions.keys()) + 1
            self.add_partition(new_pid)
            return new_pid
        return min_partition

    def start_consumer_server(self, partition_id):
        port = self.base_consumer_port + partition_id
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((self.host, port))
            server.listen()
            print(f"Broker Consumer Server for Partition {partition_id} running on {self.host}:{port}")
            while True:
                client_socket, addr = server.accept()
                print(f"Consumer connected to Partition {partition_id} from {addr}")
                with self.lock:
                    self.consumer_sockets[partition_id] = client_socket
                threading.Thread(target=self.handle_consumer, args=(partition_id, client_socket), daemon=True).start()

    def handle_consumer(self, partition_id, client_socket):
        while True:
            with self.lock:
                if not self.partitions[partition_id].empty() and self.consumer_sockets[partition_id]:
                    try:
                        _, _, msg = self.partitions[partition_id].get()
                        self.send_to_consumer(partition_id, msg)
                    except Exception as e:
                        print(f"Error in handle_consumer {partition_id}: {e}")
                        self.consumer_sockets[partition_id] = None
                        break
            time.sleep(1)

if __name__ == "__main__":
    broker = MessageBroker(host='192.168.151.51')  # Thay bằng IP máy bạn
    input("Press Enter to exit...\n")