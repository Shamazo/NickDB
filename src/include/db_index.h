#ifndef INDEX_H
#define INDEX_H

#include "main_api.h"


//Helper function to find where to insert new values in sorted base data
size_t clustered_insert_position(int* vals, size_t lp, size_t rp, int key);
// ****************************************************************************
// Sorted indexes 
// ****************************************************************************

/*
* A sorted index, either clustered or unclustered
* keys is an array of the index keys
* positions is a position list such that col_positions[i] has the position for keys[i]
* length, the number of keys/col_positions 
* allocated_size, the allocated size of lenght and col_positions as a multiple of sizeof()
*/
typedef struct SortedIndex {
    int* keys;
    size_t* col_positions;  
    size_t length;  
    size_t allocated_size;
} SortedIndex;

/*
* These two functions create the index structures. For a unclustered index
* the index is constructed on data in the existing column.
* for a clustered index, we assume as per specs that no data is in the table
* and will be loaded later.
* These functions modify the index field of the column struct. 
*/

int create_unclustered_sorted_index(Column* column);
int create_clustered_sorted_index(Column* column);

/*
* These two functions load new data into an index
* They do NOT load an exitsing index from disk. 
* the clustered function WILL move data in the table
*/

int load_into_unclustered_sorted_index(Column* column);
int load_into_clustered_sorted_index(Table* table, Column* column);

/*
* In addition to bulk load functions, I may need insert functions 
*/

int sorted_clustered_insert(SortedIndex* index, int val, size_t pos);
int sorted_unclustered_insert(SortedIndex* index, int val, size_t pos);

int sorted_clustered_delete(SortedIndex* index, size_t pos);
int sorted_unclustered_delete(SortedIndex* index, size_t pos);
/*
* Main Sorted select function to be used in db_operators.c
*/
Result* sorted_range_select(SortedIndex* sorted_index, int lower, int upper);


// ****************************************************************************
// Btrees
// ****************************************************************************


#define NODE_SIZE 1024
// actual size is 992 bytes which is < 16 cache lines
#define FAN_OUT 80 // this makes num_elements and positions/child_pointers each fit on 


/*
* The index pointer in the column will point to the root of the tree. 
* union of either array of pointers to children nodes or array of positions
* this means the positions/child pointers are exactly 10 cache lines 
* the vals are 5 and the meta data fits in the last cache line
*/
struct BNode;
typedef struct BNode {	
	union {
		size_t positions[FAN_OUT+1];
		struct BNode* child_pointers[FAN_OUT+1];
	} children;
	int vals[FAN_OUT];
	size_t num_elements;
	struct BNode* previous; // only used in leaf nodes
	struct BNode* next;  
	bool is_leaf;
	bool is_root;
} BNode;



/*
* Traverse a the tree in BFS order applying func to each node.
* func does not have to use out_file, but it is used for saving nodes
* to disk. 
*/

void traverse_tree(BNode* root, void (*func)(BNode*, FILE*), FILE* out_file);
/*
* prints a node to standard out, handles both inner nodes and leaves. 
*/
void print_node(BNode* bnode, FILE* file);

/*
* Sets the column struct to appropriate values for type of index
* called before loading data in.
* unclustered also creates the btree if data is in the column.
*/
int create_unclustered_btree_index(Column* column);
int create_clustered_btree_index(Column* column);

/*
* These two functions load new data into an index
* They do NOT load an exitsing index from disk. 
* the clustered function WILL move data in the table
*/

int load_into_unclustered_btree_index(Column* column);
int load_into_clustered_btree_index(Table* table, Column* column);


int btree_clustered_insert(BNode* root, int val, size_t pos);
int btree_unclustered_insert(BNode* root, int val, size_t pos);

int btree_clustered_delete(BNode* root, int val);
int btree_unclustered_delete(BNode* root, int val);


Result* BTree_unclustered_select(BNode* root, size_t max_num_vals, int lower, int upper);

Result* BTree_clustered_select(BNode* root, int lower, int upper);



// ****************************************************************************
// Helper functions 
// ****************************************************************************


/*
* Sort a table in place based on given column
*/
void sort_table_on_column(Table* table, Column* column);



#endif