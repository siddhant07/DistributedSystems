from config import replica_cnt, eventual_port, HOST
import random
from datetime import datetime
import zmq

print("WELCOME")
print("You can enter the commands get/set in the following format (command) (key) (value)")

# Connect to the any replica selected at random
port_id = random.choice(range(0, replica_cnt))
this_port = eventual_port + port_id
#print(this_port)
context = zmq.Context()

client_socket = context.socket(zmq.REQ)
client_socket.connect(f"tcp://{HOST}:{this_port}")

while(True):
    command = input("Enter command:- ")

    if(command.strip().startswith("set") or command.strip().startswith("get")):
        parts = command.split(" ")
        #Send values to server
        client_socket.send_string(command)

        # Receive a response from the server
        response = client_socket.recv(1024).decode()
        print(f"Received from server: {response} T" ,datetime.now().strftime("%H:%M:%S"))

    elif(command == 'quit'):
        break

client_socket.close()