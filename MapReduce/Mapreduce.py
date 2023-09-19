import subprocess
import os
from config import Mapper_output_dir, Reducer_input_dir, Reducer_output_dir

# Get a list of all files in the directory
files1 = os.listdir(Mapper_output_dir)
files2 = os.listdir(Reducer_input_dir)
files3 = os.listdir(Reducer_output_dir)

# Loop through each file and remove it
for file in files1:
    filepath = os.path.join(Mapper_output_dir, file)
    os.remove(filepath)

for file in files2:
    filepath = os.path.join(Reducer_input_dir, file)
    os.remove(filepath)

for file in files3:
    filepath = os.path.join(Reducer_output_dir, file)
    os.remove(filepath)

process1 = subprocess.Popen(["python", "functionalities.py"])
process2 = subprocess.Popen(["python", "master.py"])

process1.wait()
process2.wait()