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
There are two main structures for the database itself: Tables and Columns. Tables encapsulate columns and also store necessary metadata, such as name, number of columns etc. Column structs are stored within an array in the Table struct. To look up a column a linear search is done on the column array. Each column contains a pointer to an array where the data is stored along with metadata such as name, allocated space and length. A database can have any number of tables, limited by system memory. Each Table can have any number of columns, but the number of columns is declared on Table creation. To find a column within a table a linear search is done on the column array.  There is also a database struct which keeps track of tables. To find a Table a linear search is done on the tables in the database. Linear search is clearly not the most efficient solution, but since a low number of tables and columns are expected it would be premature optimization to use a more complex structure to allow for faster lookup.

#### Persistence 
The database struct, Table structs and Column structs are directly written to a single binary catalogue file. The data in each column is a separate memory mapped file and is flushed on shutdown. To ensure that all data is in memory we use the `mlockall(MCL_FUTURE);` system call on startup. 

### Scans
Basic scans are implemented with tight for loops. Scans also support scanning a column, but only selecting elements in a position list that can be passed as an argument. 

Example tight for loop implementation designed to reduce branching. 
```$xslt
for (size_t i=0; i < num_elements; i++){
    result[j] = i;
    j += ((column[i] >= min_val) & (column[i] < max_val));
}
```

#### Shared scans
We support batches of scans, where a user can declare a batch of selects to be run concurrently. Shared scans are implemented by looping over elements in the column and for each element we loop over each predicate from the batched selects to check which predicates are met by the element. 
To speed this up, we pre-process the batch of selects into sub-batches. Each of which is spun out to a thread. If the number of sub-batches is greater than the number of allowable threads then  we wait for the threads to finish their current batches before spinning up more threads to do the next set of batches.
To make this more cache efficient we apply vectorization. 
This should make the operation faster as all threads share the L3 cache. If they were not vectorized some threads may be faster than others leading to a mismatch in requested data leading to cache thrashing as threads compete for cache space.

### Indexing
#### Sorted Indices
A clustered sorted index is simply a table sorted on the column with the index. An unclustered sorted index is a sorted version of the underlying column along with a positions lists holding the location of every value in the sorted column in the underlying data. Binary search is used to find the lower and upper bounds of any select operator. For a clustered index the select operator returns positions between the lower and upper bounds, and for an unclustered index the select operator returns the positions found in the position list between starting at the lower bound and ending at the upper bound.
A select on a column which uses an secondary sorted index returns the positions, but they are not sorted. This is an important consideration if these positions are going to be used in additional operators. For example fetching values from a column with non increasing positions will be random memory rather than contiguous and therefore liable to be much slower. 
#### B-Trees

Each node of the B+-Trees stores positions or child pointers, depending on whether the node is a leaf or an internal node, the values associated with those positions or child pointers and metadata: the previous and next nodes for children as well as booleans for whether the node is a leaf or is the root. I decided to use the same type of node for internal and leaf nodes, because the metadata overhead is minimal; 18 bytes or 24 if you include padding for alignment. In internal nodes, the value is the lowest value in the subtree of the corresponding pointer. The structure of clustered and unclustered nodes is the same, but clustered b+trees sort the underlying data first and so have consecutive positions in the leaves. Binary search is used within nodes to find the highest value lower than the lower bound. In unclustered B-trees we scan through nodes to the right of the leaf with the lowest value until we find a value which does not match the predicate, copying positions into our result as we go. For clustered B-trees we do a point query for the positions of the lower bound and higher bound and return range between the two. 
The fanout of the tree is a tunable parameter. My initial design was to have nodes which used a small multiple of the cache line size. A fanout of 80 gives nodes which are 992 bytes and are slightly less than 16 cache lines. The values and pointers/positions are stored in separate arrays to reduce cache misses as we apply binary search to the values, and then only access one cache line of pointers/positions. 

### Joins 
#### Nested Loop Joins
The nested loop join iterates through the left column and for each value iterates through the right column comparing values. For matching values the positions of the left and right table are saved to the left and right result position vectors.

#### Hash joins
The basic design of the hash join is to partition the columns to be joined by a hash so that the partitions are small enough to fit in the cache. Each pair of partitions is then joined by constructing a hash table on the smaller partition of the pair and probing the hash table with each value of the larger side of the partition. For any match, the resulting left and right positions are written to the left and right result position vectors. 
To do the partitioning I used a radix based algorithm. We make some power of 2 number of partitions using the lowest bits. The number of partitions to make is a tunable parameter and should be selected to reduce the number of TLB misses. I use a separate thread to partition the left and right join columns. The partitions are stored in a struct that contains an array of the left and right values as well as the left and right positions for each partition as well as the length of each partition. 
Once we have the partitions we can perform the join one pair of partitions at a time. To construct my hash tables I use static hashing with two pass counting. In the first pass I count how many values fall into each “bucket”, these counts then become the offsets for the start of each bucket in a contiguous array. There is one array for values, and a second array for the corresponding positions.  On the second pass we then populate the hash table by copying the value and positions to the correct location in the arrays. To retrieve items, we hash the probe value and then do a linear search on the part of the array corresponding to the bucket. I set the number of buckets to be the number of values/4, because when we search through the bucket we load a cache line at a time up the memory hierarchy in the CPU and each cache line stores 64/4 = 16 ints. If the data was known to be perfectly uniform we would set the number of buckets to be size/16. Since non-uniform data is likely we leave some overhead, but also may suffer degraded performance for extremely skewed data.  To parallelize this algorithm, I spin off a thread for each pair of partitions, upto some tunable limit. If there are more partitions than threads we run the join in batches until all partitions have been joined. After each batch of partitions is done we copy the results into the final left and right result arrays. 

### Updates 
I use a differential structure to batch inserts, updates and deletes. This structure holds the positions that are too be deleted and the values which are to be inserted. Updates are a delete followed by an insert. After a regular scan is completed on the base data a function is called to update the result with the pending inserts, deletes and updates.  The positions and values are currently implemented as an array and linear search is used to find positions to delete and inserts which match the predicate to add to the result. It would be much faster to probe a hashtable for the delete positions and insert values, but I did not have time to implement this. 
The size of the differential structure is a tunable parameter. Once the number of inserts or deletes reaches the defined size of the differential structure, deletes and inserts are flushed to the base data and indexes. There is a separate function to insert and delete for each type of index. The position of the deletion, or the new position of the tuple in the base data are passed into these index functions, so that they can increment/decrement all of the positions greater than the change. 

## Usage
### Docker and building  
To spin up a docker container run ./docker_bash.sh from the root directory of this repo. The entry point
is an interactive bash shell. This also mounts the data, src and tests directories inside the container. 
This project uses a simple CMake build system. The root CMakeLists.txt is in src. To build cd to src, configure cmake
with `cmake .` then build with `make`. This will place the binaries in src/build. 

### API 

## Todos
- [ ] Document API
- [ ] Fix logging, make it consistent and more readable. 
- [ ] Handle more edge cases with readable error codes 
- [ ] Fork and convert to c++
- [ ] Add support for variable length data types / strings. Possibly storing offset (and length?) of the string in a strings file in the columns. 
- [ ] Write an SQL front end to parse SQL and produce a query plan
- [ ] write a basic optimizer
- [ ] Get the overall codebase into a shape where we can run existing benchmarks such as TPC-H