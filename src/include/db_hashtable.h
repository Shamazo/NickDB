#ifndef DB_HASH_TABLE
#define DB_HASH_TABLE

#include <stdio.h>
#include <stdlib.h>

/* This is a static hash two pass counting hash table
* It does not support updates since it does not need to in order to be used for joins
* Keys are stored in a single contiguous array and matching positions are stored in another
* contiguous array. The hash table is built by passing over the data once, in the first pass
* offsets for each  "bucket" are calculated by counting how many items match each bucket
* in the second pass we then insert items into the arrays.
* num_buckets is set to size / 4 since when we search though a bucket we load a cache line at a 
* time up the memory hierarchy and each cache line stores 64/4 = 16 ints. If the data were perfectly 
* uniform we would have size/16 buckets, but non uniform data is likely so we should have some overhead.
* although buckets are not constructed to be cacheline size aligned. 
*/
typedef struct HashTable {
	int* keys;
	size_t* positions;
	size_t* offsets;
	size_t* bucket_counts;
	size_t* bucket_offsets; // used as we load the table, offset past start of bucket to insert next item
    size_t size; // length of keys and positions in # of items
    size_t num_buckets;
} HashTable;



HashTable* ht_allocate(size_t size); // allocates hashtable struct and datastructures 
int bulk_ht_load(HashTable* ht, int* keys, size_t* values, size_t num_values);
// returns all matching values in hashtable in values and number in num_values
int get(HashTable* ht, int key, size_t** values, size_t* num_values, size_t* value_init_length);
int ht_deallocate(HashTable* ht);
#endif

