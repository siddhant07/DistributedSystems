import zmq
from constants import PORT, TYPE, SERVER_CNT, START_SERVER_PORT, HOST
import random
from datetime import datetime

def send_kv_pair(socket, key, value):
    msg = f"set {key} {value}"
    socket.send_string(msg)
    response = socket.recv()
    print(f"Received from server: {response} T" ,datetime.now().strftime("%H:%M:%S"))

def get_value(socket, key):
    msg = f"get {key}"
    socket.send_string(msg)
    response = socket.recv().decode().replace('END', '')
    print(f"Received from server: {response} T" ,datetime.now().strftime("%H:%M:%S"))

def is_server_running(server_port):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    try:
        socket.connect(f"tcp://{HOST}:{server_port}")
        return True
    except zmq.error.ZMQError:
        return False
    finally:
        socket.close(linger=0)
        context.term()

def start_client():
    context = zmq.Context()

    # pick a random number between 1 to SERVER_CNT and assign the client to it
    random_server = random.randint(0, SERVER_CNT - 1)
    server_port = START_SERVER_PORT + random_server

    if not is_server_running(server_port):
        print(f"Server {server_port} is not running")
        return

    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{HOST}:{server_port}")
    print(f"Client connected to server {server_port} T",datetime.now().strftime("%H:%M:%S"))

    cmd = input("Enter command: ")

    if cmd.strip().startswith("set"):
        parts = cmd.split(" ")
        key = parts[1]
        value = parts[2]
        send_kv_pair(socket, key, value)

    elif cmd.strip().startswith("get"):
        parts = cmd.split(" ")
        key = parts[1]
        get_value(socket, key)

    socket.close()
    context.term()

if __name__ == "__main__":
    start_client()
