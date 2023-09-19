import multiprocessing
import zmq
import threading
import os
from constants import DATA_DIR,SERVER_CNT,START_SERVER_PORT,HOST,START_BROADCAST_PORT
from random import random
from time import sleep
from datetime import datetime


class Server:
    def __init__(self, host, port, kv_file_name,broadcastPort):
        self.host = host
        self.port = port
        self.kv_file_name = kv_file_name
        self.broadcastPort=broadcastPort
  
    def handle_client(self,socket,lock,broadcastPort,publisher):
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
            print(f"Server on {self.port} responded back to client T",datetime.now().strftime("%H:%M:%S") )
            self.start_broadcast(msg,broadcastPort,publisher)
                
        socket.close()
        # print("socket closed")

    def start_broadcast(self,msg,broadcastPort,publisher):
        print(f"Server {self.port} stated broadcasting T",datetime.now().strftime("%H:%M:%S") )
        for i in range(SERVER_CNT-1):
            publisher.send_string(msg)  

    def start_server(self):
        # create context and sockets
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        publisher = context.socket(zmq.PUB)
        # subscriber = context.socket(zmq.SUB)

        print(f"Server started listening on {self.port} T",datetime.now().strftime("%H:%M:%S") )
        # bind sockets to ports
        socket.bind(f"tcp://*:{self.port}")
        publisher.bind(f"tcp://*:{self.broadcastPort}")
        # subscriber.connect(f"tcp://{HOST}:{self.broadcastPort}")
        # subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

        # create lock for thread safety
        lock = threading.Lock()

        # start client handler thread
        client_thread = threading.Thread(target=self.handle_client, args=(socket, lock, self.broadcastPort, publisher,))
        client_thread.start()

        
        # wait for threads to finish
        client_thread.join()
        # subscriber_thread.join()

    def handle_subscriber(self, subscriber):
        while True:
            for i in range(SERVER_CNT):
                if START_SERVER_PORT+i!=self.port:
                    message = subscriber.recv_string()
                    port=START_SERVER_PORT+i
                    print(f"Server {port} received broadcasted message '{message}' ",datetime.now().strftime("%H:%M:%S") )
                    parts = message.split(" ")
                    action = parts[0].lower()
                    fileName="key_value_store_"+str(port)+".txt"
                    # print(fileName)
                    file_path = os.path.join(DATA_DIR, fileName)
                    if action == "set":
                        key = parts[1]
                        value = parts[2]
                        flag = False
                        with open(file_path, "a+") as f:
                            sleep(20)
                            f.seek(0)
                            lines = f.readlines()
                            for i, line in enumerate(lines):
                                if line.startswith(key + ":"):
                                    # response="NOT STORED"
                                    print(f"Server {port} replica already had key:{key} value:{value} T",datetime.now().strftime("%H:%M:%S") )
                                    flag = True
                            if flag:
                                pass 
                            else:
                                f.write(f"{key}:{value}\n")
                                # response="STORED" 
                                print(f"Server {port} updated key:{key} value:{value} T",datetime.now().strftime("%H:%M:%S") )


if __name__ == "__main__":
    print("Listening ...")
    servers = []
    processes = []
    for i in range(SERVER_CNT):
        KV_file_name = f"key_value_store_{START_SERVER_PORT+i}.txt"
        server = Server(HOST, START_SERVER_PORT+i, KV_file_name,START_BROADCAST_PORT+i)
        servers.append(server)
        # p = multiprocessing.Process(target=server.start_server ,args=(HOST,START_SERVER_PORT+i,START_BROADCAST_PORT+i,))
        p = multiprocessing.Process(target=server.start_server)
        processes.append(p)
        p.start()

        contextSub = zmq.Context()
        subscriber = contextSub.socket(zmq.SUB)
        subscriber.connect(f"tcp://{HOST}:{START_BROADCAST_PORT+i}")
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
        subscriber_thread = threading.Thread(target=server.handle_subscriber, args=(subscriber,))
        subscriber_thread.start()

    for p in processes:
        p.join()
