import socket
import threading

from config import mapper_count, Map_port_starter, reducer_count, Reducer_port_starter, Mapper_output_dir, Reducer_input_dir, Reducer_output_dir
from collections import defaultdict
import os
import re

mapper_threads=[]
reducer_threads=[]

def kv_converter(line):
    words=line.split()
    for word in words:
        word=re.sub('[^A-Za-z]+', '', word)
        word=word.lower()
        yield(word,1)

def hashFunc(word_length):
    return word_length % reducer_count

def mapper(input, port):
    print("In mapper \n")
    filename="MapperOutput_"+str(port-Map_port_starter)+".txt"
    for i in input:
        with open(i, encoding="utf-8", errors='ignore') as input_file:
            input_data=input_file.read()

        mapped_data=[]
        for line in input_data.splitlines():
            for key_value_pair in kv_converter(line):
                mapped_data.append(key_value_pair)

        grouped_data = defaultdict(list)

        for key, value in mapped_data:
            grouped_data[key].append(value)

        output_path = os.path.join(Mapper_output_dir, filename)
        with open(output_path, "w") as output_file:
            for key, values in grouped_data.items():
                if(len(key)>0):
                    output_file.write("{} {}:{}\n".format(key, i, sum(values)))
        output_file.close() 
        input_file.close() 
    return True

def group_by(mapper_output, reducer_count):
    output_files = [open(Reducer_input_dir + f"ReducerInput_{i}.txt", "w") for i in range(reducer_count)]

    for filename in os.listdir(mapper_output):
        with open(os.path.join(mapper_output, filename), "r") as f:
            for line in f:
                key, value = line.strip().split(" ")  
                output_file = output_files[hashFunc(len(key))]
                output_file.write(f"{key} {value}\n")

    for output_file in output_files:
        output_file.close()

def reducer(id):
    print(f"In reducer{id} \n")
    input_filename = Reducer_input_dir+"ReducerInput_"+id+".txt"
    output_filename = os.path.join(Reducer_output_dir+"ReducerOutput_"+id+".txt")

    key_values = {}
    with open(input_filename, 'r') as input_file:
        for line in input_file:
            key, value = line.strip().split()
            fileFoundIn, count = value.strip().split(':')

            if key in key_values:
                key_values[key] += int(count)
            else:
                key_values[key] = int(count)

    with open(output_filename, 'w') as output_file:
        for key, value in key_values.items():
            output_file.write(f"{key} {value}\n")
    return True

def reducer_inverted_idx(id):
    print(f"In reducer{id} \n")
    input_filename = Reducer_input_dir+"ReducerInput_"+id+".txt"
    output_filename = os.path.join(Reducer_output_dir+"ReducerInvertedOutput_"+id+".txt")

    key_values = {}
    with open(input_filename, 'r') as input_file:
        for line in input_file:
            key, value = line.strip().split(':')

            if key in key_values:
                key_values[key] += int(value)
            else:
                key_values[key] = int(value)

    with open(output_filename, 'w') as output_file:
        for key, value in key_values.items():
            output_file.write(f"{key}/{value}\n")
    return True

def mappper_assignment(port):
    print("In Mapper assignment for port", port, "\n")
    mapper_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mapper_socket.bind(('',port))
    mapper_socket.listen(10)
    print("Listening on port ", port, "\n")
    connection, address = mapper_socket.accept()
    data=connection.recv(1024)
    input=eval(data.decode())
    #print(input)
    ack="ACK"

    if(mapper(input, port)):
        #print("In if")
        connection.send(ack.encode())

    return True

def reducer_assignment(port):
    print("In Reducer assignment for port", port, "\n")
    pass
    reducer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    reducer_socket.bind(('',port))
    reducer_socket.listen(1)
    print("Listening on port ", port, "\n")
    connection, address = reducer_socket.accept()
    message=connection.recv(1024)
    message=message.decode()
    # print(message)
    incoming_port, reducer_id = message.strip().split(" ")
    
    ack="ACK"

    if(int(incoming_port)==port and reducer(reducer_id) and reducer_inverted_idx(reducer_id)):
        print("In if for ", incoming_port)
        connection.send(ack.encode())

    return True


def mapreduce():
    for i in range(0, mapper_count):
        t=threading.Thread(target=mappper_assignment, args=(Map_port_starter+i,))
        mapper_threads.append(t)

    for t in mapper_threads:
        t.start()
    
    for t in mapper_threads:
        t.join()

    print("Grouping the intermediate files according to hash function")
    group_by(Mapper_output_dir, reducer_count)

    print("Waiting for Command to initiate reducer")

    for i in range(0, reducer_count):
        t=threading.Thread(target=reducer_assignment, args=(Reducer_port_starter+i,))
        reducer_threads.append(t)

    for t in reducer_threads:
        t.start()
    
    for t in reducer_threads:
        t.join()
 

if __name__=='__main__':
    mapreduce()