import socket
import time

HOST = '10.0.228.126'  # IP Broker
PORT = 12345

def send_message(partition_key, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(f"PRODUCER:{partition_key}:{message}".encode())
        print(f"[PRODUCER] Đã gửi: {message} (Key: {partition_key})")

if __name__ == "__main__":
    # Ví dụ: 2 Producer gửi message với key khác nhau
    for i in range(1, 6):
        send_message(partition_key="LAPTOP", message=f"Message-{i}-from-{socket.gethostname()}")
        time.sleep(1)