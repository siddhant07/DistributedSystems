import multiprocessing
import zmq
import threading
import os
from config import HOST, linearizable_port, kv_store_name, primary_port, linearizable_dir, replica_cnt, linearizable_prim_port, broadcast_port
from time import sleep
from datetime import datetime


class PrimaryServer:
    def __init__(self, host, port, kv_store, broadcastPort):
        self.host = host
        self.port = port
        self.kv_store = kv_store
        self.broadcastPort = broadcastPort
        self.id=1
        
    def handle_client(self, client_socket, lock, broadcastPort, primary_comm):
        try:
            while True:
                command = client_socket.recv_string()

                if not command:
                    break

                parts = command.split(" ")
                cmd = parts[0].lower()
                storage_path = os.path.join(linearizable_dir, self.kv_store)
                resp = ""
                
                if cmd == "set":
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

                    #broadcast the write to all replicas
                    self.start_broadcast(command, broadcastPort, primary_comm)

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
                
        except KeyboardInterrupt:
            pass

        finally:      
            client_socket.close()

    def start_broadcast(self,msg, broadcastPort, primary_comm):
        print(f"Primary Server {self.port} stated broadcasting " )
        
        for i in range(replica_cnt):
            primary_comm.send_string(msg)  

    def start_primary_server(self):
        # create context and sockets
        context = zmq.Context()
        client_socket = context.socket(zmq.REP)
        primary_comm = context.socket(zmq.PUB)

        print(f"Primary Server started listening on {self.port} " )

        client_socket.bind(f"tcp://*:{self.port}") 
        primary_comm.bind(f"tcp://*:{self.broadcastPort}")

        lock = threading.Lock()

        client_thread = threading.Thread(target=self.handle_client, args=(client_socket, lock, self.broadcastPort, primary_comm,))
        client_thread.start()       
        
        for i in range(replica_cnt):
            
            replica_port = linearizable_prim_port + i
            replica_thread = threading.Thread(target=self.handle_replica, args=(self.broadcastPort, primary_comm, replica_port,))
            replica_thread.start()

        client_thread.join()

        for i in range(replica_cnt):
            replica_thread.join()

    def handle_replica(self, broadcastPort, primary_comm, replicaPort):
        try:
            while True:
                contextRep = zmq.Context()
                replica_socket = contextRep.socket(zmq.REP)
                replica_socket.bind(f"tcp://*:{replicaPort}")
                command = replica_socket.recv_string()
                resp = ""
                
                if command.startswith("set"):
                    parts = command.split(" ")
                    cmd = parts[0].lower()
                    storage_path = os.path.join(linearizable_dir, self.kv_store)

                    if cmd == "set":
                        key = parts[1]
                        value = parts[2]
                        found_flag = False

                        with open(storage_path, "a+") as file:
                            file.seek(0)
                            lines = file.readlines()

                            for i, line in enumerate(lines):
                                if line.startswith(key + ":"):
                                    resp = "NOT STORED"
                                    found_flag = True
                            
                            if not found_flag:
                                file.write(f"{key}:{value}\n")
                                resp="STORED" 

                    self.start_broadcast(command, broadcastPort, primary_comm)
                
                else:
                    print("Primary server received ack from replica",replicaPort)
        except KeyboardInterrupt:
            pass

        finally:
            replica_socket.close()

if __name__ == "__main__":
    print("Listening ...")
    kv_store = f"key_value_store_{primary_port}.txt"
    server = PrimaryServer(HOST, primary_port, kv_store, broadcast_port)
    p = multiprocessing.Process(target=server.start_primary_server)
    p.start()

    p.join()