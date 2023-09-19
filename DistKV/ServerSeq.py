import multiprocessing
import zmq
import threading
import os
from constants import DATA_DIR,SERVER_CNT,START_SERVER_PORT,HOST,START_PRI_CONNECT_PORT,PRI_BROADCAST_PORT
from random import random
from time import sleep
from datetime import datetime
import heapq


class Server:
    def __init__(self, host, port, kv_file_name,priConnPort):
        self.host = host
        self.port = port
        self.kv_file_name = kv_file_name
        self.priConnPort=priConnPort
        self.id = 1
        self.q = []
  
    def handle_client(self,socketClient,socketPrimary,lock,priConnPort):
        while True:
            #receiving client request
            msg = socketClient.recv_string()
            if not msg:
                break
            parts = msg.split(" ") 
            action = parts[0].lower()        
            file_path = os.path.join(DATA_DIR, self.kv_file_name)
            response = ""

            #forward to primary server first
            self.q.append(self.port)
            # print(self.q)
            if action == "set":
                print(f"Sending to primary {msg}")
                socketPrimary.send_string(msg)
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
            if action == "get":
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
            # sleep(10)
            socketClient.send_string(response)
            print(f"Server on {self.port} responded back to client T",datetime.now().strftime("%H:%M:%S") )
        socketClient.close()

          
    def handle_subscriber(self, subscriber):
        while True:
            message = subscriber.recv_string()
            print(f"Server {self.port} replica received message from primary")
            self.start_update(message)

            # create a new REQ socket for sending the acknowledgement
            context = zmq.Context()
            ack_socket = context.socket(zmq.REQ)
            ack_socket.connect(f"tcp://{HOST}:{self.priConnPort}")
            ack_socket.send_string("Sending ack")
            print(f"Server {self.port} replica sends the ack")
            ack_socket.close()


 
    def start_update(self,message):
        parts = message.split(" ")
        action = parts[0].lower()
        fileName="key_value_store_"+str(self.port)+".txt"
        file_path = os.path.join(DATA_DIR, fileName)
        response=""
        if action == "set":
            key = parts[1]
            value = parts[2]
            flag = False
            with open(file_path, "a+") as f:
                sleep(5)
                f.seek(0)
                lines = f.readlines()
                for i, line in enumerate(lines):
                    if line.startswith(key + ":"):
                        # print(f"Server {self.port} replica already had key:{key} value:{value}" )
                        flag = True
                        response="NOT STORED"
                if flag:
                    pass 
                else:
                    f.write(f"{key}:{value}\n")
                    print(f"Server {self.port} updated" )
                    response="STORED"


    def start_server(self):
        # create context and sockets
        context = zmq.Context()
        socketClient = context.socket(zmq.REP)
        # socketClient1 = context.socket(zmq.REP)
        socketPrimary = context.socket(zmq.REQ)
        # prisubscribe = context.socket(zmq.SUB)
        # self.q.append(socketClient)
        print(f"Server started listening on {self.port} " )
        # bind sockets to ports
        socketClient.bind(f"tcp://*:{self.port}")
        # socketClient1.bind(f"tcp://*:{self.port+10}")
        socketPrimary.connect(f"tcp://{HOST}:{self.priConnPort}")

        contextSub = zmq.Context()
        prisubscribe = contextSub.socket(zmq.SUB)
        prisubscribe.connect(f"tcp://{HOST}:{PRI_BROADCAST_PORT}")
        prisubscribe.setsockopt_string(zmq.SUBSCRIBE, "")

        # create lock for thread safety
        lock = threading.Lock()

        # start client handler thread
        client_thread = threading.Thread(target=self.handle_client, args=(socketClient,socketPrimary,lock,self.priConnPort))
        client_thread.start()

        subscriber_thread = threading.Thread(target=self.handle_subscriber, args=(prisubscribe,))
        subscriber_thread.start()

        # wait for threads to finish
        client_thread.join()
        # subscriber_thread.join()


if __name__ == "__main__":
    print("Listening ...")
    servers = []
    processes = []
    replicaClientPort=[]
    replicaPriPort=[]
    q=[]
    for i in range(SERVER_CNT):
        KV_file_name = f"key_value_store_{START_SERVER_PORT+i}.txt"
        server = Server(HOST, START_SERVER_PORT+i, KV_file_name,START_PRI_CONNECT_PORT+i)
        servers.append(server)
        p = multiprocessing.Process(target=server.start_server)
        processes.append(p)
        p.start()

        # context = zmq.Context()
        # socketClient1 = context.socket(zmq.REP)
        # socketClient1.bind(f"tcp://*:{START_SERVER_PORT+i+10}")

        # contextSub = zmq.Context()
        # subscriber = contextSub.socket(zmq.SUB)
        # subscriber.connect(f"tcp://{HOST}:{PRI_BROADCAST_PORT}")
        # subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
        # subscriber_thread = threading.Thread(target=server.handle_subscriber, args=(subscriber,))
        # subscriber_thread.start()

    for p in processes:
        p.join()
