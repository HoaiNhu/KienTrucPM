import socket

HOST = '10.0.228.126'  # IP Broker
PORT = 12345

def consume_from_partition(partition_id):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print(f"[CONSUMER] Đang đọc từ Partition {partition_id}...")
        while True:  # Vòng lặp vô hạn để nhận message
            try:
                s.sendall(f"CONSUMER:{partition_id}".encode())
                data = s.recv(1024).decode()
                if not data:
                    print("[CONSUMER] Broker đóng kết nối")
                    break
                print(f"[CONSUMER-P{partition_id}] Đã nhận: {data}")
            except ConnectionResetError:
                print("[CONSUMER] Broker ngắt kết nối đột ngột")
                break
            except Exception as e:
                print(f"[CONSUMER] Lỗi: {e}")
                break

if __name__ == "__main__":
    # Mỗi Consumer chọn 1 partition (0, 1, hoặc 2)
    consume_from_partition(partition_id=0)