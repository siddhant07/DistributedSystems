import subprocess

print("\n")
print("WELCOME TO THE DISTRIBUTED KEY VALUE STORE SERVER \n")
print("Sequential Consistency:- 1")
print("Linearizable Consistency:- 2")
print("Eventual Consistency:- 3")
print("Causal Consistency:- 4")

selected_type = int(input("Enter the code of type of consistency you want to implement:- "))
print("\n")

if selected_type == 1:
    process1 = subprocess.Popen(["python3", "serverSeq.py"])
    process2 = subprocess.Popen(["python3", "Sequencer.py"])
    process1.wait()
    process2.wait()

elif selected_type == 2:
    process1 = subprocess.Popen(["python3", "serverLin.py"])
    process2 = subprocess.Popen(["python3", "Linearizer.py"])
    process1.wait()
    process2.wait()

elif selected_type == 3:
    process = subprocess.Popen(["python3", "serverEve.py"])
    process.wait()

elif selected_type == 4:
    process = subprocess.Popen(["python3", "serverCau.py"])
    process.wait()