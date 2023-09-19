import subprocess

print("\n")
print("WELCOME TO THE DISTRIBUTED KEY VALUE STORE CLIENT \n")
print("Sequential Consistency:- 1")
print("Linearizable Consistency:- 2")
print("Eventual Consistency:- 3")
print("Causal Consistency:- 4")

selected_type = int(input("Enter the code of type of consistency you want to implement:- "))
print("\n")

if selected_type == 1:
    process = subprocess.Popen(["python3", "clientSeq.py"])
    process.wait()

elif selected_type == 2:
    process = subprocess.Popen(["python3", "clientLin.py"])
    process.wait()

elif selected_type == 3:
    process = subprocess.Popen(["python3", "clientEve.py"])
    process.wait()

elif selected_type == 4:
    process = subprocess.Popen(["python3", "clientCau.py"])
    process.wait()