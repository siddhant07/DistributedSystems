import zmq
import threading
from config import HOST, eventual_port, kv_store_name, Eventual_dir, replica_cnt, broadcast_port
import multiprocessing
from datetime import datetime
import threading
import os

class Server:
    def __init__(self, host, port, kv_store, broadcastPort):
        self.host = host
        self.port = port
        self.kv_store = kv_store
        self.broadcastPort = broadcastPort
        self.id = 1
        self.que = []

    def start_broadcast(self, command, broadcastPort, primary_comm):
        #print("IN BROADCAST\n")
        print(f"Server at port {self.port} has started broadcasting T", datetime.now().strftime("%H:%M:%S"))

        for i in range(replica_cnt-1):
            #print("IN BROADCAST FOR\n")
            primary_comm.send_string(command)


    def handle_client(self, client_socket, lock, broadcastPort, primary_comm):
        while True:
            command = client_socket.recv_string()
            #print("IN WHILE   \n")

            if not command:
                break

            parts = command.split(" ")
            cmd = parts[0].lower()
            storage_path = os.path.join(Eventual_dir, self.kv_store)

            if cmd == "set":
                print("IN  SET  \n")
                key = parts[1]
                value = parts[2]
                lock.acquire()
                lock_flag = False
                resp = ""

                with open(storage_path, "a+") as file:
                    file.seek(0)
                    lines = file.readlines()

                    for i, line in enumerate(lines):
                        if line.startswith(key+":"):
                            resp = "KEY ALREADY PRESENT"
                            lock_flag = True
                    
                    if not lock_flag:
                        #print("IN  WRITING  \n")
                        file.write(f"{key}:{value}\n")
                        resp = "STORED SUCCESSFULLY"
                lock.release()

            elif cmd == "get":
                key = parts[1]
                lock.acquire()
                resp = ""
                found_flag = False

                with open(storage_path, "r") as file:
                    file.seek(0)
                    lines = file.readlines()

                    if len(lines) == 0:
                        resp = "Key store is empty"

                    for i, line in enumerate(lines):
                        k, v = line.strip().split(":")
                        if k == key:
                            resp = f"Value at key {k} is {v}\n"
                            found_flag = True
                            break
                    
                    if not found_flag:
                        resp = "Key not present\n"
                lock.release()

            else:
                resp = "Invalid command"
            
            client_socket.send_string(resp)
            print(f"Server on {self.port} is sending a response to client T",datetime.now().strftime("%H:%M:%S") )
            self.start_broadcast(command, broadcastPort, primary_comm)

        client_socket.close()


    def handle_replica(self, replica_socket):
        while True:
            for i in range(replica_cnt):
                if eventual_port+i != self.port:
                    command = replica_socket.recv_string()
                    port = eventual_port+i
                    
                    print(f"Server at port {port} has received the broadcasted message '{command}' ",datetime.now().strftime("%H:%M:%S") )
                    
                    parts = command.split(" ")
                    cmd = parts[0].lower()
                    fileName = f"{kv_store_name}{str(port)}.txt"
                    storage_path = os.path.join(Eventual_dir, fileName)

                    if cmd == "set":
                        key = parts[1]
                        value = parts[2]
                        found_flag = False
                        
                        with open(storage_path, "a+") as file:
                            file.seek(0)
                            lines = file.readlines()
                            
                            for i, line in enumerate(lines):
                                if line.startswith(key + ":"):
                                    print(f"Server at port {port} already had key:{key} value:{value} T",datetime.now().strftime("%H:%M:%S") )
                                    found_flag = True
                            
                            if not found_flag:
                                file.write(f"{key}:{value}\n") 
                                print(f"Server at port {port} has added key:{key} value:{value} T",datetime.now().strftime("%H:%M:%S") )
  



    def start(self):
        context = zmq.Context()
        client_socket = context.socket(zmq.REP) #handles clients
        primary_comm = context.socket(zmq.PUB)  #is the socket used for communication between primary and replicas

        print(f"Listening on server {self.port} T",datetime.now().strftime("%H:%M:%S"))

        client_socket.bind(f"tcp://*:{self.port}")
        primary_comm.bind(f"tcp://*:{self.broadcastPort}")

        lock = threading.Lock()

        client_thread = threading.Thread(target=self.handle_client, args=(client_socket, lock, self.broadcastPort, primary_comm,))
        client_thread.start()

        client_thread.join()

if __name__=="__main__":
    servers = []
    processes = []

    files = os.listdir(Eventual_dir)       #cleans the output folder whenever the server is restarted

    for file in files:
        filepath = os.path.join(Eventual_dir, file)
        os.remove(filepath)

    for i in range(replica_cnt):
        kv_file = f"{kv_store_name}{eventual_port+i}.txt"
        server = Server(HOST, eventual_port+i, kv_file, broadcast_port+i)
        servers.append(server)
        
        process = multiprocessing.Process(target=server.start)
        processes.append(process)
        process.start()

        contextSub = zmq.Context()
        replica_socket = contextSub.socket(zmq.SUB)
        replica_socket.connect(f"tcp://{HOST}:{broadcast_port+i}")
        replica_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        replica_thread = threading.Thread(target=server.handle_replica, args=(replica_socket,))
        replica_thread.start()
    
    for process in processes:
        process.join()