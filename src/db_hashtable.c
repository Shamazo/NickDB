#include "db_hashtable.h"
#include <signal.h>
#include <assert.h>



HashTable* ht_allocate(size_t size){
	HashTable* ht = malloc(sizeof(HashTable));
	ht->keys = malloc(sizeof(int) * size);
	// raise(SIGINT);
	ht->positions = malloc(sizeof(size_t) * size);
	ht->offsets = calloc(size, sizeof(size_t));
	ht->bucket_offsets = calloc(size, sizeof(size_t));
	ht->size = size;
	ht->num_buckets = size / 4;
	// edge case for very small partitions
	if (ht->num_buckets == 0){
		ht->num_buckets += 1;
	}
	ht->bucket_counts = calloc(ht->num_buckets, sizeof(size_t));

	return ht;
}

// Adpated from  https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
static inline size_t hash_func(int x){
	x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return (size_t) x;
}

int bulk_ht_load(HashTable* ht, int* keys, size_t* vals, size_t num_values){
	assert(num_values <= ht->size);
	// step 1 count bucket size
	for (size_t i = 0; i < num_values; i++){
		// using modulo here, but could use bit shifting if num_buckets was a power of 2
		ht->bucket_counts[hash_func(keys[i]) % ht->num_buckets] += 1;
	}

	size_t curr_offset = 0;
	// calculate bucket offsets
	for (size_t i = 0; i < ht->num_buckets; i++){
		ht->offsets[i] = curr_offset;
		curr_offset += ht->bucket_counts[i];
		// printf("curr_offset %ld ith offset %ld\n", curr_offset, ht->offsets[i] );
	}

	// insert into table
	size_t offset;
	for (size_t i = 0; i < num_values; i++){
		size_t hash = hash_func(keys[i]) % ht->num_buckets;

		offset = ht->offsets[hash] + ht->bucket_offsets[hash];
		ht->keys[offset] = keys[i];
		ht->positions[offset] = vals[i];
		ht->bucket_offsets[hash] += 1;
	}

	return 1;
}
// returns all matching values in hashtable in values and number in num_values
int get(HashTable* ht, int key, size_t** values, size_t* num_values, size_t* value_init_length){
	size_t hash = hash_func(key) % ht->num_buckets;
	size_t offset = ht->offsets[hash];

	*num_values = 0;
	for (size_t i=0; i < ht->bucket_counts[hash]; i++){
		// printf("bucket %ld i: %ld, offset %ld, val %d \n", hash, i, offset, ht->keys[i+offset]);
		if (*num_values + 1 >= *value_init_length){
			// printf("increase value array \n");
			*values = realloc(*values, sizeof(size_t) * (*value_init_length + 1));
			*value_init_length = *value_init_length + 1;
		}
		(*values)[*num_values] = ht->positions[i+offset];
		*num_values = *num_values + (ht->keys[i + offset] == key);
	}

	return 1;
}


int ht_deallocate(HashTable* ht){
	free(ht->keys);
	free(ht->positions);
	free(ht->offsets);
	free(ht->bucket_offsets);
	free(ht->bucket_counts);
	free(ht);	

	return 1;
}


// Tests

#if 0
int main(void) {
    HashTable* ht = ht_allocate(50);
    
    int* vals = malloc(sizeof(int) * 50);
    size_t* pos = malloc(sizeof(size_t) * 50);

    for (size_t i =0; i < 50; i++){
    	vals[i] = (int) i;
    	pos[i] = i+ 50;
    }

    vals[10] = 3;
    vals[10] = 3;

    bulk_ht_load(ht, vals, pos, 50);

    for (size_t i = 0; i<ht->size; i++){
    	printf("i: %ld, val%d , pos %ld \n", i, ht->keys[i], ht->positions[i] );
    }


   size_t* ret_vals = malloc(sizeof(size_t));
   size_t value_init_length = 1;
   size_t num_values = 0;

   get(ht, 3, ret_vals, &num_values, &value_init_length);

   printf("numvals %ld \n", num_values );

   for (size_t i= 0; i < num_values; i++){
		printf("ret val %ld \n", ret_vals[i]);
   }


    return 0;
}
#endif