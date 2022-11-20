# Perfect Power Processing using Map Reduce

## About The Project
Multithreading program which takes as imput a set of files and finds all the perfect powers grater than 0.

## Implementation
The program was built using C++ and Pthreads Library. 
Map-Reduce mechanism:
- the main thread starts at the same time the Mapper and Reducer threads
- the Mapper threads take the input files and generate partial lists, a list containg all the numbers that have the same exponent
- Reducer threads take the partial lists and count the unique values for every exponent



