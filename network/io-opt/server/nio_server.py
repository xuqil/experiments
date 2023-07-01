import socket

HOST = ''
PORT = 50007


def server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen(10)  # 设置最大监听数目，并发
        s.setblocking(False)  # 设置为非阻塞
        clients = []  # 保存客户端 socket
        while True:
            try:
                conn, addr = s.accept()  # 非阻塞，轮询检查是否有连接
                conn.setblocking(False)
                clients.append((conn, addr))  # 存放客户端 socket
                print('Connected by', addr)
            except BlockingIOError:
                pass

            for cs, ca in clients:
                try:
                    data = cs.recv(1024)  # 接收数据，非阻塞
                    if len(data) > 0:  # 收到了数据
                        print(f"message from {ca}: {data}")
                        cs.sendall(data)
                    else:
                        cs.close()
                        clients.remove((cs, ca))
                except Exception:
                    pass


if __name__ == '__main__':
    server()
