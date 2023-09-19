import multiprocessing
import zmq
import threading
import os
from constants import DATA_DIR,SERVER_CNT,START_SERVER_PORT,HOST,PRIMARY_PORT,PRI_BROADCAST_PORT,START_PRI_CONNECT_PORT
from random import random
from time import sleep
from datetime import datetime


class PrimaryServer:
    def __init__(self, host, port, kv_file_name,broadcastPort):
        self.host = host
        self.port = port
        self.kv_file_name = kv_file_name
        self.broadcastPort=broadcastPort
        self.id=1
        
    def handle_client(self,socket,lock,broadcastPort,publisher):
        try:
            while True:
                msg = socket.recv_string()
                if not msg:
                    break
                parts = msg.split(" ")
                action = parts[0].lower()
                file_path = os.path.join(DATA_DIR, self.kv_file_name)
                response = ""
                if action == "set":
                    key = parts[1]
                    value = parts[2]
                    lock.acquire()
                    flag = False
                    with open(file_path, "a+") as f:
                        sleep(random())
                        f.seek(0)
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            if line.startswith(key + ":"):
                                response="NOT STORED"
                                flag = True
                        if flag:
                            pass 
                        else:
                            f.write(f"{key}:{value}\n")
                            response="STORED" 
                    lock.release()

                    #broadcast the write to all replicas
                    self.start_broadcast(msg,broadcastPort,publisher)

                elif action == "get":
                    key = parts[1]
                    lock.acquire()
                    with open(file_path, "r") as f:
                        sleep(random())
                        data = f.readlines()
                        if len(data)==0:
                            response="KEY NOT FOUND"
                        for line in data:
                            k, v = line.strip().split(":")
                            if k == key:
                                response = f"VALUE {key} {len(v)}\r\n{v}\r\nEND\r\n"
                                break
                        else:
                            response = "KEY NOT FOUND"
                    lock.release()
                else:
                    response="Invalid"
                socket.send_string(response)
                print(f"Server on {self.port} responded back to client " )
                
        except KeyboardInterrupt:
            pass

        finally:      
            socket.close()
        # print("socket closed")

    def start_broadcast(self,msg,broadcastPort,publisher):
        print(f"Primary Server {self.port} stated broadcasting " )
        for i in range(SERVER_CNT):
            publisher.send_string(msg)  

    def start_pri_server(self):
        # create context and sockets
        context = zmq.Context()
        clientsocket = context.socket(zmq.REP)
        publisher = context.socket(zmq.PUB)

        print(f"Primary Server started listening on {self.port} " )
        # bind sockets to ports
        clientsocket.bind(f"tcp://*:{self.port}") 
        publisher.bind(f"tcp://*:{self.broadcastPort}")
        
        # create lock for thread safety
        lock = threading.Lock()

        # start client handler thread
        client_thread = threading.Thread(target=self.handle_client, args=(clientsocket, lock, self.broadcastPort, publisher,))
        client_thread.start()

        # start replica handler thread
        
        for i in range(SERVER_CNT):
            # print("Primary connected to replica",replicasocket)
            replicaPort=START_PRI_CONNECT_PORT+i
            replicaConn_thread = threading.Thread(target=self.handle_replica, args=(self.broadcastPort, publisher,replicaPort,))
            replicaConn_thread.start()

        client_thread.join()
        for i in range(SERVER_CNT):
            replicaConn_thread.join()

    def handle_replica(self,broadcastPort,publisher,replicaPort):
        try:
            while True:
                contextRep = zmq.Context()
                replicasocket = contextRep.socket(zmq.REP)
                replicasocket.bind(f"tcp://*:{replicaPort}")
                msg=replicasocket.recv_string()
                # print("Primary received msg",msg)
                response = ""
                if msg.startswith("set"):
                    print("Primary server received from replica",msg)
                    parts = msg.split(" ")
                    action = parts[0].lower()
                    file_path = os.path.join(DATA_DIR, self.kv_file_name)

                    if action == "set":
                        key = parts[1]
                        value = parts[2]
                        # lock.acquire()
                        flag = False
                        with open(file_path, "a+") as f:
                            sleep(random())
                            f.seek(0)
                            lines = f.readlines()
                            for i, line in enumerate(lines):
                                if line.startswith(key + ":"):
                                    response="NOT_STORED"
                                    flag = True
                            if flag:
                                pass 
                            else:
                                f.write(f"{key}:{value}\n")
                                response="STORED" 
                        # lock.release()
                        print(response)
                    # new_msg=msg+" "+response
                    self.start_broadcast(msg,broadcastPort,publisher)
                else:
                    print("Primary server received ack from replica",replicaPort)
                        # replicasocket.send_string(response)
        except KeyboardInterrupt:
            pass

        finally:
            replicasocket.close()
            # context.term()

if __name__ == "__main__":
    print("Listening ...")
    KV_file_name = f"key_value_store_{PRIMARY_PORT}.txt"
    server = PrimaryServer(HOST, PRIMARY_PORT, KV_file_name,PRI_BROADCAST_PORT)
    p = multiprocessing.Process(target=server.start_pri_server)
    p.start()

    p.join()