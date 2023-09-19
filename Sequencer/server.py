import socket
from config import start_port, process_count, HOST, sequencer_port
import multiprocessing
import heapq



class Server:
    def __init__(self, host, port, sequencer_host):
        self.host = host
        self.port = port
        self.sequencer_host = sequencer_host
        self.id = 1
        self.que = []

    def start(self):
        heapq.heapify(self.que)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen(10)
            print(f"Listenning on server: {self.port}")

            while True:
                conn, addr = server_socket.accept()
                print(f"Sequencer is sending request for port {self.port} from ", addr)
                newID = conn.recv(1024).decode()
                heapq.heappush(self.que, [int(newID), addr])
                self.request_handler(conn, addr)

    def request_handler(self, conn, addr):
        try:
            while len(self.que) > 0 and self.que[0][0] == self.id:
                c, ip = heapq.heappop(self.que)
                conn.send(str(self.id).encode())
                self.id += 1
            conn.close()

        except Exception as e:
            print(f"Error handling request from {addr}: {e}")
            conn.close()

if __name__=="__main__":
    servers = [Server(HOST, start_port+i, sequencer_port) for i in range(process_count)]
    processes = [multiprocessing.Process(target=server.start) for server in servers]
    for process in processes:
        process.start()
