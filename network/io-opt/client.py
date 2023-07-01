import socket
import threading

HOST = '127.0.0.1'  # The remote host
PORT = 50007  # The same port as used by the server


def client(n: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        msg = f'Hello, world[{n}]'
        s.sendall(msg.encode(encoding="utf-8"))
        data = s.recv(1024)
    print('Received', repr(data))


def main():
    tasks = []
    # 5 个并发的请求
    for i in range(5):
        task = threading.Thread(target=client, args=(i,))
        tasks.append(task)

    for task in tasks:
        task.start()

    for task in tasks:
        task.join()


if __name__ == '__main__':
    main()
