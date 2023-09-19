STEPS TO EXECUTE MAPREDUCE:

1. Insert all the files you want to MapReduce in the Input File directory in .txt format
2. Decide the number of Mappers you want and Reducers you want and change the values in the config.py file accordingly
3. Run the command MapReduce.py file


OUTPUT:
1. There are two types of output files generated:
    a. ReducerInvertedOutput_{ReducerID}.txt:- this contains the output in inverted index format
    b. ReducerOutput_{ReducerID}.txt:- this contains the output in word-count format