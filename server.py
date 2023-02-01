import socket
import threading
from time import sleep
from random import random

PORT = 6969
TYPE = 'utf-8'

def client_handler(client_socket, lock):
    commands=client_socket.recv(1024).decode()
    params=commands.split(" ")
    action=params[0].lower()

    response= ""
    
    if action == "set":
        key=params[1]
        value=params[2]
        lock.acquire()

        with open("storage_for_pair.txt", "r") as f:
            sleep(random())
            data=f.readlines()
            for i, j in enumerate(data):
                if j.startswith(key+":"):
                    response="NOT-STORE\r\n"
                    
            with open("storage_for_pairs.txt", "a") as f:
                f.write(f"{key}:{value}\n")
                reponse="STORED\r\n"
        lock.release()
    
    elif action == "get":
        key=params[1]
        lock.acquire()
        with open("storage_for_pair.txt", "r") as f:
            sleep(random())
            data=f.readlines()
            for j in enumerate(data):
                i, k =j.strip().split(":")
                if i == key:
                    response = f"VALUE {value} {len(k)}\r\n{k}\r\nEND\r\n"
                else:
                    response="KEY NOT FOUND"
        lock.release()
    
    else:
        response="Invalid"

    lock.release()
    client_socket.send(bytes(response, TYPE))
    client_socket.close()


def startServer():
    server_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    IP=socket.gethostname()
    server_socket.bind(IP, PORT)
    server_socket.listen(5)
    lock=threading.Lock()

    while True:
        client_socket, address=server_socket.accept()
        print("Recieved a connection from %s" %str(address))
        client_thread=threading.Thread(target=client_handler, args=(client_socket, lock))
        client_thread.start()