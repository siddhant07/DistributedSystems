import socket
PORT=6942
TYPE='utf-8'

def get_func(sock_code, key):
    command = f"get {key}"
    sock_code.send(command.encode())
    response = sock_code.recv(1024).decode()
    print("Server said:  %s" %response)

def send_func(sock_code, key, value):
    command = f"set {key} {value}"
    sock_code.send(command.encode())
    response = sock_code.recv(1024).decode()
    print(response)
    print("Server said:  %s" %response)

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    IP = socket.gethostname()    
    command = input("What should I execute?: ")    
    client_socket.connect((IP, PORT))   
    
    if command.strip().startswith("set"):
        params = command.split(" ")
        key = params[1]
        value = params[2]
        send_func(client_socket, key, value)

    elif command.strip().startswith("get"):
        params=command.split(" ")
        key=params[1]
        get_func(client_socket,key)

    client_socket.close()

if __name__ == "__main__":
    start_client()