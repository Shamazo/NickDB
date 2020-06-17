# NickDB

## Introduction
This is a column oriented database which I originally implemented for cs165 at Harvard. If you are a current student at Harvard and are taking, or thinking of taking, cs165 I direct you to the honor code and you should stop reading here. 
I am in the process of extending this code further and plan to port it to C++ as an exercise to learn c++.


## Design
This is not, and will not be a database for production use. It is designed to run on a single machine and to allow for experimentation. 
### Client Server model 
The client and server communicate using unix sockets. This could be changed to us TCP sockets to allow for different machines to run the client and server, but that is beyond the scope of this project. 
The client server communication API is defined in message.h. 

### storage model 
TO EXPAND

Columns are stored in individual files. Tables are composed of multiple columns. All metadata lives in the catalogue.
### Scans
TO EXPAND

### Indexing
TO EXPAND 

B-Trees and sorted indexes are supported both for primary and secondary indexes. 

### Joins 
TO EXPAND

Hash joins and nested loop joins are supported. Hash joins uses Radix partitioning and two pass static hashing.
### Updates 
TO EXPAND

Updates are supported and use a secondary write store. Periodically this write store is flushed to the main column store. 


## Usage
### Docker and building  
To spin up a docker container run ./docker_bash.sh from the root directory of this repo. The entry point
is an interactive bash shell. This also mounts the data, src and tests directories inside the container. 
This project uses a simple CMake build system. The root CMakeLists.txt is in src. To build cd to src, configure cmake
with `cmake .` then build with `make`. This will place the binaries in src/build. 

### API 

## Todos
- [ ] Flesh out readme
- [ ] Fix logging, make it consistent and more readable. 
- [ ] Handle more edge cases with readable error codes 
- [ ] Fork and convert to c++