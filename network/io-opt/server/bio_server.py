import socket

HOST = ''
PORT = 50007


def server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(1)
        while True:
            conn, addr = s.accept()  # 会阻塞在此，直到又客户端连接
            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)  # 会阻塞在此，直到收到客户端的请求
                    if not data: break
                    print(f"message from {addr}: {data}")
                    conn.sendall(data)


if __name__ == '__main__':
    server()
