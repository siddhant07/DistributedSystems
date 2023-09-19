import socket
from config import sequencer_port


# Connect to the server
while(True):
    data = input("Type 'get' to trigger get_id() or 'quit' to exit: ")

    if(data == 'get'):
        # Create a socket object
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('localhost', sequencer_port))
        #print('Connected to server')

        client_socket.sendall(data.encode())

        # Receive a response from the server
        response = client_socket.recv(1024).decode()
        print(f'Received id: {response}')
        client_socket.close()

    elif(data == 'quit'):
        break