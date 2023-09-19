import zmq
import os
from config import HOST, sequential_port, kv_store_name, Sequential_dir, replica_cnt, sequential_prim_port, broadcast_port
import multiprocessing
import threading
from datetime import datetime



class Server:
    def __init__(self, host, port, kv_store, sequential_host):
        self.host = host
        self.port = port
        self.kv_store = kv_store
        self.sequential_host = sequential_host
        self.id = 1
        self.que = []

    def update_store(self, command):
        parts = command.split(" ")
        cmd = parts[0].lower()

        filename = f"{kv_store_name}{self.port}.txt"
        storage_path = os.path.join(Sequential_dir, filename)
        resp = ""

        if cmd == "set":
            key = parts[1]
            value = parts[2]
            found_flag = False

            with open(storage_path, "a+") as file:
                file.seek(0)
                lines = file.readlines()

                for i, line in enumerate(lines):
                    if line.startswith(key + ":"):
                        found_flag = True
                        resp = "KEY ALREADY EXISTS"
                
                if not found_flag:
                    file.write(f"{key}:{value}\n")
                    print(f"Server replica at port {self.port} is updated")
                    resp = "STORED SUCCESSFULLY"

    def handle_replica(self, primary_comm):
        while True:
            command = primary_comm.recv_string()
            print(f"Server at port {self.port} has recived the message from primary")
            self.update_store(command)

            contextREQ = zmq.Context()
            ack_sock = contextREQ.socket(zmq.REQ)
            ack_sock.connect(f"tcp://{HOST}:{self.sequential_host}")
            ack_sock.send_string("ACK")
            print(f"Server at port {self.port} has sent the acknowledgement")
            ack_sock.close()

    def handle_client(self, client_socket, primary_socket, lock, sequencer_host):
        while True:
            command = client_socket.recv_string()

            if not command:
                break

            parts = command.split(" ")
            cmd = parts[0].lower()
            storage_path = os.path.join(Sequential_dir, self.kv_store)
            resp = ""

            self.que.append(self.port)

            if cmd == "set":
                print(f"Forwarding request to client")
                primary_socket.send_string(command)
                key = parts[1]
                value = parts[2]
                lock.acquire()
                lock_flag = False
                
                with open(storage_path, "a+") as file:
                    file.seek(0)
                    lines = file.readlines()

                    for i, line in enumerate(lines):
                        if line.startswith(key + ":"):
                            resp = "KEY ALREADY EXISTS"
                            lock_flag = True
                    
                    if not lock_flag:
                        file.write(f"{key}:{value}\n")
                        resp = "STORED SUCCESSFULLY"

                lock.release()
            
            elif cmd == "get":
                key = parts[1]
                lock.acquire()
                found_flag = False

                with open(storage_path, "r") as file:
                    lines = file.readlines()
                    if len(lines) == 0:
                        resp = "Key store is empty"
                    
                    for i, line in enumerate(lines):
                        k, v = line.strip.split(":")
                        if k == key:
                            resp = f"Value for key {k} is {v}\n"
                            found_flag = True
                            break
                    
                    if not found_flag:
                        resp = "Key not present\n"
                lock.release()

            else:
                resp = "Invalid"

            client_socket.send_string(resp)
            print (f"Server on port {self.port} sends message back to client T",datetime.now().strftime("%H:%M:%S"))
            client_socket.close()                        



    def start(self):
        context = zmq.Context()
        client_socket = context.socket(zmq.REP) #handles clients
        primary_socket = context.socket(zmq.REQ) #handles primary server

        print(f"Listening on port {self.port} T", datetime.now().strftime("%H:%M:%S"))
        client_socket.bind(f"tcp://*:{self.port}")
        primary_socket.connect(f"tcp://{HOST}:{self.sequential_host}")

        contextCon = zmq.Context()
        primary_comm = contextCon.socket(zmq.SUB)  #is the socket used for communication between primary and replicas
        primary_comm.connect(f"tcp://{HOST}:{broadcast_port}")
        primary_comm.setsockopt_string(zmq.SUBSCRIBE, "")

        lock = threading.Lock()

        client_thread = threading.Thread(target=self.handle_client, args=(client_socket, primary_socket, lock, self.sequential_host))
        client_thread.start()

        replica_thread = threading.Thread(target=self.handle_replica, args=(primary_comm,))
        replica_thread.start()

        client_thread.join()

if __name__=="__main__":
    servers = []
    processes = []

    files = os.listdir(Sequential_dir)       #cleans the output folder whenever the server is restarted

    for file in files:
        filepath = os.path.join(Sequential_dir, file)
        os.remove(filepath)

    for i in range(1, replica_cnt+1):
        kv_file = f"{kv_store_name}{sequential_port+i}.txt"
        server = Server(HOST, sequential_port+i, kv_file, sequential_prim_port+i)
        servers.append(server)
        
        process = multiprocessing.Process(target=server.start)
        processes.append(process)
        process.start()
    
    for process in processes:
        process.join()