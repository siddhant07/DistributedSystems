import socket
import time
import concurrent.futures
from config import start_port, process_count, HOST, sequencer_port
import subprocess


class Sequencer:
    def __init__(self, host, port, server_cnt):
        self.host = host
        self.port = port
        self.id = 1
        self.server_cnt = server_cnt

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as seq_socket:
            seq_socket.bind((self.host, self.port))
            seq_socket.listen()
            print("Sequencer is now accepting connections...")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                while True:
                    conn_sock, (conn, addr) = seq_socket.accept()
                    print("Getid Req Received from client", addr, conn)
                    message = str(self.id)
                    executor.submit(self.broadcast, message, conn_sock)
                    self.id += 1


    def broadcast(self, message, connection_socket):
        def tell_all(port):
            serverName = HOST
            serverSocket = port
            print("Broadcasting to server", serverSocket)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as clientSocket:
                clientSocket.connect((serverName, serverSocket))
                clientSocket.send(message.encode())
                msg = clientSocket.recv(1024)
                print(port, "Server is sending the id", msg)
                try:
                    connection_socket.send(msg)
                    print("And back to client")
                    time.sleep(2.0)
                except:
                    pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=process_count) as executor:
            executor.map(tell_all, range(start_port, start_port + process_count))



if __name__ == "__main__":
    process = subprocess.Popen(["python3", "server.py"])
    seq = Sequencer(HOST, sequencer_port, process_count)
    seq.start()
    process.wait()