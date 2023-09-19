import socket
import threading
import os

from config import mapper_count, Map_port_starter, input_dir, reducer_count, Reducer_port_starter

threads_mapper=[]
threads_reducer=[]
groupfilenames={}

def data_distribution(input_dir):
    countFiles=0
    filenames=[]
    for f in os.listdir(input_dir):
        if os.path.isfile(os.path.join(input_dir, f)):
            countFiles+=1
            filenames.append(input_dir+"/"+f)

    print(filenames)
    
    if(countFiles>=mapper_count):
        distCount=countFiles//mapper_count
        remaining=countFiles % distCount
    else:
        distCount=1
        remaining=0
    
    group=[distCount]*mapper_count
    group[-1]=distCount+remaining

    start=0
    for i in range(len(group)):
        end=start+group[i]
        groupfilenames[i]=filenames[start:end]
        start=end
    return groupfilenames

def mapper_initiator(data, port):
    mapper_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(port)
    servername=socket.gethostbyname(socket.gethostname())
    mapper_socket.connect((servername, port))
    mapper_socket.send(str(data).encode())
    print("Data sent on ", port, "\n")

    ack=mapper_socket.recv(1024)
    ack=ack.decode()
    print(ack," for port ", port, "\n")
    mapper_socket.close()
    return ack=="ACK"

def reducer_initiator(port, count):
    reducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(port)
    servername=socket.gethostbyname(socket.gethostname())
    reducer_socket.connect((servername, port))
    message=str(port)+" "+str(count)
    # print(message)
    reducer_socket.send(message.encode())
    print("Command sent on ", port, "\n")

    ack=reducer_socket.recv(1024)
    ack=ack.decode()
    print("ACK for port ", port, "\n")
    reducer_socket.close()
    return ack=="ACK"

def master(input_dir):
    chunks = data_distribution(input_dir)    
    
    for i in range(0, mapper_count):
        t=threading.Thread(target=mapper_initiator, args=(chunks[i], Map_port_starter+i))
        threads_mapper.append(t)

    for t in threads_mapper:
        t.start()
    
    for t in threads_mapper:
        t.join()

    print("Congratulations! The mapping phase is complete\n")
    print("Initiating Reducer Phase\n")

    

    for i in range(0, reducer_count):
        t=threading.Thread(target=reducer_initiator, args=(Reducer_port_starter+i, i))
        threads_reducer.append(t)

    for t in threads_reducer:
        print("Starting threads\n")
        t.start()
    
    for t in threads_reducer:
        t.join()

    print("Congratulations, we have successfully completed MapReduce over the specified input")




if __name__=='__main__':
    master(input_dir)

        