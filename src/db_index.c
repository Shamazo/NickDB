#include <string.h>
#include <assert.h>
#include <signal.h>
#include "db_index.h"
#include "main_api.h"
#include "utils.h"

/*
* Macros for sorting
*/
#define pivot_index() (begin+(end-begin)/2)
#define swap(a,b,t) ((t)=(a),(a)=(b),(b)=(t))

/*
* This function sorts both data and positions based on data in place 
* only supports ints. 
*/


void db_qsort(int* data, size_t* positions, int begin, int end) {
   int pivot;
   //temp swap variable
   int t;  
   if (end > begin) {
      int l = begin + 1;
      int r = end;
      swap(data[begin], data[pivot_index()], t); /*** choose arbitrary pivot ***/
      swap(positions[begin], positions[pivot_index()], t); /*** choose arbitrary pivot ***/
      pivot = data[begin];
      while(l < r) {
         if (data[l] <= pivot) {
            l++;
         } else {
            while(l < --r && data[r] >= pivot) /*** skip superfluous swaps ***/
               ;
            swap(data[l], data[r], t); 
            swap(positions[l], positions[r], t); 
         }
      }
      l--;
      swap(data[begin], data[l], t);
      swap(positions[begin], positions[l], t);
      db_qsort(data, positions, begin, l);
      db_qsort(data, positions, r, end);
   }
}


void pos_qsort(size_t* data, int begin, int end) {
   size_t pivot;
   //temp swap variable
   size_t t;  
   if (end > begin) {
      size_t l = begin + 1;
      size_t r = end;
      swap(data[begin], data[pivot_index()], t); /*** choose arbitrary pivot ***/
      pivot = data[begin];
      while(l < r) {
         if (data[l] <= pivot) {
            l++;
         } else {
            while(l < --r && data[r] >= pivot) /*** skip superfluous swaps ***/
               ;
            swap(data[l], data[r], t); 
         }
      }
      l--;
      swap(data[begin], data[l], t);
      pos_qsort(data, begin, l);
      pos_qsort(data, r, end);
   }
}

/*
* input is a column with values implicitly in positions 1-n in array order
* positions is a position list which maps old positions to new position.
* so column->data[i] goes to new_data[positions[i]]
* I can't think of a way to do this without a ton of random access on at least one of the inputs
*/

void reorder_column_on_position(Column* column, size_t* positions, size_t allocated_length){
    //only support int data currently
    int* temp = malloc(sizeof(int) * allocated_length);
    for (size_t i = 0; i < *column->num_rows; i++){
        temp[i] = column->data[positions[i]];
    }
    
    //column->data is a memmapped file, so we cant just change the pointer 
    memcpy(column->data, temp, sizeof(int) * allocated_length);
    free(temp);

    return;
}

/*
* This function sorts the entire table on the input column
* This is in place and will modify both table and column
*/

void sort_table_on_column(Table* table, Column* column){
    if (*column->num_rows > 0){
        size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
        for (size_t i = 0; i < *column->num_rows; i++){
            positions[i] = i;
        }
    
    db_qsort(column->data, positions, 0, *column->num_rows);

    // for (size_t i = 0; i < *column->num_rows; i++){
    //     printf("pos: %ld, val: %d \n", positions[i], column->data[i]);
    // }

    for (size_t i=0; i < table->col_count; i++) {
        if (strcmp(table->columns[i].name, column->name) != 0)
            reorder_column_on_position(&table->columns[i], positions, table->table_alloc_size);
        }
    }

    return;
}

/*
* Returns the position where the key would be inserted into the sorted array vals
*/
size_t clustered_insert_position(int* vals, size_t lp, size_t rp, int key) {
   if (lp <= rp ) {
        size_t mid = lp + (rp - lp) / 2;
        if (vals[mid] == key ) {
            return mid;
        } else if ((vals[mid-1] <= key && (mid == rp ))){ 
            return mid -1;
        } else if (vals[mid] > key) {
            if (mid == 0) {
                return mid;
            } else if (vals[mid - 1] < key){
                return mid - 1;
            }
            // recurse left 
            return clustered_insert_position(vals, lp, mid - 1, key);
        }
        // recurse right 
        return clustered_insert_position(vals, mid + 1, rp, key);
   }
   return 0;
}

// ****************************************************************************
// Sorted indexes 
// ****************************************************************************


int create_unclustered_sorted_index(Column* column){
    column->clustered = false;
    column->index_type = SORTED;
    SortedIndex* index = malloc(sizeof(SortedIndex));
    // There is data we need to build an unclustered index on
    if (*column->num_rows > 0){
        int* data = malloc(sizeof(int) * (*column->num_rows));
        size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
        for (size_t i = 0; i < *column->num_rows; i++){
            positions[i] = i;
        }
        
        memcpy(data, column->data, *(column->num_rows) * sizeof(int));

    
        db_qsort(data, positions, 0, *column->num_rows);
        for (size_t i = 0; i < *column->num_rows; i++){
            printf("%ld  %d \n",positions[i], data[i] );
        }
        index->keys = data;
        index->col_positions = positions;
    } else {
        index->keys = NULL;
        index->col_positions = NULL;
    }

    
    index->length = *column->num_rows;
    index->allocated_size = *column->num_rows;
    column->index = index;
    return 0;
}

/*
* Bulk load into a unclustered sorted index
*/
int load_into_unclustered_sorted_index(Column* column){

    SortedIndex* index = (SortedIndex*) column->index;
    // There is data we need to build an unclustered index on
    if (*column->num_rows > 0){
        int* data = malloc(sizeof(int) * (*column->num_rows));
        size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
        for (size_t i = 0; i < *column->num_rows; i++){
            positions[i] = i;
        }
        
        memcpy(data, column->data, *(column->num_rows) * sizeof(int));
        db_qsort(data, positions, 0, *column->num_rows);
        index->keys = data;
        index->col_positions = positions;
    } else {
        index->keys = NULL;
        index->col_positions = NULL;
    }    
    index->length = *column->num_rows;
    index->allocated_size = *column->num_rows;
    return 0;
}


/*
* When we create a clustered index, we make the assumption that the table is empty
* and that load_clustered_sorted_index will be called after the data as been loaded into the table
*/

int create_clustered_sorted_index(Column* column){
    column->clustered = true;
    column->index_type = SORTED;
    SortedIndex* index = malloc(sizeof(SortedIndex));
    // There is data we need to build an unclustered index on
    index->length = 0;
    index->allocated_size = 0;
    column->index = index;
    return 0;
}


int load_into_clustered_sorted_index(Table* table, Column* column){
    column->clustered = true;
    column->index_type = SORTED;
    SortedIndex* index = (SortedIndex*) column->index;
    size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
    // printf("Loading into clustered sorted index \n");
    for (size_t i = 0; i < *column->num_rows; i++){
        positions[i] = i;
    }
    
    sort_table_on_column(table, column);
    // printf("Column \n");
    
    // store keys for clustered as well, as we may end up supporting updates
    // using a differential structure, so column-> data may end up being different
    // from index->keys
    // index->keys = malloc(sizeof(int) * (*column->num_rows));
    // memcpy(index->keys, column->data, (*column->num_rows) * sizeof(int));
    index->keys = column->data;
    index->col_positions = positions;
    index->length = *column->num_rows;
    index->allocated_size = *column->num_rows;
    for (size_t i = 0; i < *column->num_rows; i++){
            printf("%ld  %d \n", positions[i], index->keys[i] );
    }
    
    return 0;
}

/*
* Sorted Binary search, recursive implementation. Returns the position of the elemnent
* with key=key if found otherwise returns 0 and sets bool* success to false;
*/
size_t sorted_binary_search(int* data, size_t lp, size_t rp, int key, bool* success) {
   if (lp <= rp ) {
        size_t mid = lp + (rp - lp) / 2;
        printf("%d\n",  data[mid]);
        if (data[mid] == key) {
            *success = true;
            return mid;
        } else if (data[mid] > key) {
            if (mid == 0 || data[mid - 1] < key) {
                *success = true;
                return mid;
            }
            // recurse left 
            return sorted_binary_search(data, lp, mid - 1, key, success);
        }
        // recurse right 
        return sorted_binary_search(data, mid + 1, rp, key, success);
   }
   // No data found matching key
   *success = false;
   return 0;
}



Result* sorted_range_select(SortedIndex* sorted_index, int lower, int upper){
    bool success = false;
    // for (size_t i=0; i < sorted_index->length; i++){
    //     printf(" keys %d\n", sorted_index->keys[i]);
    // }
    raise(SIGINT);
    size_t low_idx = sorted_binary_search(sorted_index->keys, 0, sorted_index->length, lower, &success);
    size_t high_idx = sorted_binary_search(sorted_index->keys, 0, sorted_index->length, upper, &success);

    //ensure we include all things with the lower key, and not just the first one binary search finds
    // order matters here, need to check >0 before checking previous element
    while (low_idx > 0 && sorted_index->keys[low_idx - 1] >= lower){
        low_idx -= 1;
    }

    while (high_idx > 0 && sorted_index->keys[high_idx - 1] >= upper && high_idx >= low_idx ) {
        high_idx -= 1;
    }
    printf("High idx: %ld, low_idx: %ld \n", low_idx, high_idx);
    Result* result = malloc(sizeof(Result));
    result->num_update_tuples = 0;
    result->num_tuples = high_idx - low_idx;
    result->data_type = POSITIONLIST;
    result->payload = malloc(sizeof(size_t) * result->num_tuples);
    memcpy((size_t*)result->payload, &sorted_index->col_positions[low_idx], sizeof(size_t) * result->num_tuples);

    return result;
}


// need to implement all of these
int sorted_clustered_insert(SortedIndex* index, int val, size_t pos){
    (void) val;
    (void) pos;
    // inserting into base data handles most of this
    index->length += 1;
    index->col_positions = realloc(index->col_positions, sizeof(size_t) * index->allocated_size + 1);
    index->col_positions[index->length] = index->length;
    index->length += 1;
    return 1;
}
int sorted_unclustered_insert(SortedIndex* index, int val, size_t pos){
    if (index->length + 1 >= index->allocated_size){
        index->keys = realloc(index->keys, sizeof(int) * index->allocated_size + 20);
        index->col_positions = realloc(index->col_positions, sizeof(size_t) * index->allocated_size + 20);
        index->allocated_size += 20;
    }
    index->keys[index->length] = val;
    index->col_positions[index->length] = pos;
    index->length += 1;
    db_qsort(index->keys, index->col_positions, 0, index->length);
    index->length += 1;
    return 1;
}

int sorted_clustered_delete(SortedIndex* index, size_t pos){
    memmove(&index->keys[pos], &index->keys[pos+1], (index->length - pos) * sizeof(size_t));
    for (size_t i = pos; i < index->length-1; i++){
        index->col_positions[i] -= 1;
    }
    index->length -= 1;
    return 1;
}
int sorted_unclustered_delete(SortedIndex* index, size_t pos){
    size_t index_pos = 0;
    for (size_t i = 0; i < index->length; i++){
        if (index->col_positions[i] == pos){
            index_pos = i;
        }
        if (index->col_positions[i] > pos){
            index->col_positions[i] -= 1;
        }
    }

    memmove(&index->keys[index_pos], &index->keys[index_pos+1], (index->length - index_pos) * sizeof(size_t));
    index->length -= 1;
    return 1;
}



// ****************************************************************************
// Btrees
// ****************************************************************************


/*
* given sorted data and positions builds a linked list of the leaf nodes
*/
BNode* build_leaves(int* data, size_t* positions, size_t data_length){
    BNode* curr_node = calloc(1, sizeof(BNode));
    curr_node->previous = NULL;
    curr_node->is_leaf = true;
    curr_node->is_root = false; 
    BNode* head = curr_node;

    size_t internal_idx = 0;
    size_t i=0;
    while (i < data_length){
        assert(curr_node->num_elements <= FAN_OUT);
        curr_node->vals[internal_idx] = data[i];
        curr_node->children.positions[internal_idx] = positions[i];
        curr_node->num_elements += 1; 
        internal_idx += 1;

        if (internal_idx == FAN_OUT){
            internal_idx = 0;
            BNode* next_node = calloc(1, sizeof(BNode));
            next_node->is_leaf = true;
            next_node->is_root = false; 
            curr_node->next = next_node;
            curr_node = next_node;
            curr_node->next = NULL;
        }
        i++;
    }

    return head;
}



/*
* Takes in a linked list of the previous layer and constructs the next level up
*/
BNode* build_layer(BNode* head_node, size_t* nodes_in_layer){
    BNode* curr_node = calloc(1, sizeof(BNode));
    BNode* first_node = curr_node;
    curr_node->previous = NULL;
    curr_node->is_leaf = false;
    curr_node->is_root = false;
    BNode* next_node;
    size_t i = 0;
    while (head_node != NULL){
        assert(curr_node->num_elements <= FAN_OUT);
            if (curr_node->num_elements == FAN_OUT-1){
                // bug?
                if (head_node != NULL){
                   curr_node->children.child_pointers[FAN_OUT] = head_node->next; 
                } else {
                    curr_node->children.child_pointers[FAN_OUT] = NULL;
                }

                next_node = calloc(1, sizeof(BNode));
                next_node->previous = curr_node;
                next_node->is_leaf = false;
                next_node->is_root = false;
                curr_node->next = next_node;
                curr_node = next_node;
                i = 0;  
                *nodes_in_layer += 1;              
            }

            curr_node->children.child_pointers[i] = head_node;
            curr_node->vals[i] = head_node->vals[0];
            head_node = head_node->next;
            curr_node->num_elements += 1;            
            i++;
    }
    return first_node;
}

// build a btree given sorted data and accompanying positons. 
BNode* build_btree(int* data, size_t* positions, size_t data_length){
    BNode* head_node = build_leaves(data, positions, data_length);
    size_t nodes_in_layer = 0;
    while (true){
        head_node = build_layer(head_node, &nodes_in_layer);
        // printf("The nodes in the layer go %ld \n", nodes_in_layer );
        if (nodes_in_layer == 0){
            head_node->is_root = true;
            return head_node;
        } else {
            nodes_in_layer = 0;
        }
    }
}

int create_unclustered_btree_index(Column* column){
    printf("create unclustered btree \n");
    column->clustered = false;
    column->index_type = BTREE;
    column->index = NULL;
    // First we need to sort our data if any has been loaded 
    if (*column->num_rows > 0){
        int* data = malloc(sizeof(int) * (*column->num_rows));
        size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
        for (size_t i = 0; i < *column->num_rows; i++){
            positions[i] = i;
        }
        
        memcpy(data, column->data, *(column->num_rows) * sizeof(int));
        db_qsort(data, positions, 0, *column->num_rows);
        BNode* root = build_btree(data, positions, *column->num_rows);
        column->index = root;
    } 
    return 0;
}


int load_into_unclustered_btree_index(Column* column){
    if (column->clustered == true || column->index_type != BTREE){
        log_err("%s:%d Attempt to call load unclustered btree on clustered btree index", __FILE__, __LINE__);
        return -1;
    }
    // First we need to sort our data if any has been loaded 
    if (*column->num_rows > 0){
        int* data = malloc(sizeof(int) * (*column->num_rows));
        size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
        for (size_t i = 0; i < *column->num_rows; i++){
            positions[i] = i;
        }
        
        memcpy(data, column->data, *(column->num_rows) * sizeof(int));
        db_qsort(data, positions, 0, *column->num_rows);
        BNode* root = build_btree(data, positions, *column->num_rows);
        column->index = root;
    } 

    // traverse_tree(column->index, print_node, NULL);
    return 0;
}


/*
* Still working with restriction that clustered indices must be created
* before the loading of the data. 
*/

int create_clustered_btree_index(Column* column){
    column->clustered = true;
    column->index_type = BTREE;
    column->index = NULL;
    return 0;
}


int load_into_clustered_btree_index(Table* table, Column* column){
    if (column->clustered == false || column->index_type != BTREE){
        log_err("%s:%d Attempt to call load clustered btree on non clustered btree index", __FILE__, __LINE__);
        return -1;
    }
    // First we need to sort our data if any has been loaded 
    size_t* positions = malloc(sizeof(size_t) * (*column->num_rows));
    for (size_t i = 0; i < *column->num_rows; i++){
        positions[i] = i;
    }
    
    sort_table_on_column(table, column);


    printf("load clustered btree\n");
    if (*column->num_rows > 0){
        int* data = malloc(sizeof(int) * (*column->num_rows));
        memcpy(data, column->data, *(column->num_rows) * sizeof(int));
        BNode* root = build_btree(data, positions, *column->num_rows);
        column->index = root;
        // traverse_tree(column->index, print_node, NULL);
    } 
    return 0;
}



void print_node(BNode* bnode, FILE* file){
    (void) file;
    printf("META: is_leaf %d, num_elements %ld\n Vals: ", bnode->is_leaf, bnode->num_elements);
    for (size_t i = 0; i < bnode->num_elements; i++){
        printf("%d ", bnode->vals[i]);
    }

    if (bnode->is_leaf){
        printf("\n, Pos: ");
        for (size_t i = 0; i < bnode->num_elements+1; i++){
            printf("%ld ", bnode->children.positions[i]);
        }

    } else {
        printf("\n, Ptrs: ");
        for (size_t i = 0; i < bnode->num_elements+1; i++){
            printf("%p ", (void*) bnode->children.child_pointers[i]);
        }
    }
    printf("\n");
    return;

}


/*
* BFS traversal of the btree starting at root, applys function f to each node
*/

void traverse_tree(BNode* root, void (*func)(BNode*, FILE*), FILE* out_file){
    size_t seen_nodes = 1;
    size_t curr_idx = 0;

    // node_ptrs is an array of pointers to all of the nodes, we incrementally realloc 
    // as we have more pointer to add to it. Starts with just the root
    BNode** node_ptrs = malloc(sizeof(BNode*));
    node_ptrs[0] = root;

    while (curr_idx < seen_nodes){
        BNode* curr_node = node_ptrs[curr_idx];
        if (curr_node == NULL){
            curr_idx += 1;
            continue;
        }

        if (curr_node->is_leaf == false){
            size_t ptrs_in_node = curr_node->num_elements + 1; // +1 is the rightmost edge pointer 
            node_ptrs = realloc(node_ptrs, sizeof(BNode*) * (ptrs_in_node + seen_nodes));
            memcpy(&node_ptrs[seen_nodes], curr_node->children.child_pointers, sizeof(BNode*) * ptrs_in_node);
            seen_nodes += ptrs_in_node;
        }
        curr_idx += 1;
        func(curr_node, out_file);
    }
    return;
}



#define MIN(X, Y) (((X) < (Y)) ? (X) : (Y))


/*
* Want this to return the position of the lower bound of the key
*/

size_t BNode_binary_search(BNode* bnode, size_t lp, size_t rp, int key, bool* success) {
   if (lp <= rp ) {
        size_t mid = lp + (rp - lp) / 2;
        if (bnode->vals[mid] == key ) {
            *success = true;
            return mid;
        } else if ((bnode->vals[mid-1] <= key && (mid == rp ))){ // || bnode->vals[MIN(mid+1, rp)] > key
            *success = true;
            return mid -1;
        } else if (bnode->vals[mid] > key) {
            if (mid == 0) {
                *success = true;
                return mid;
            } else if (bnode->vals[mid - 1] < key){
                *success = true;
                return mid - 1;
            }
            // recurse left 
            return BNode_binary_search(bnode, lp, mid - 1, key, success);
        }
        // recurse right 
        return BNode_binary_search(bnode, mid + 1, rp, key, success);
   }
   // Value not found, return lower bound
   *success = false;
   return 0;
}



/*
* Passes tests untop test27 inclusive with size 100
*/
Result* BTree_unclustered_select(BNode* root, size_t max_num_vals, int lower, int upper){
    Result* result = malloc(sizeof(Result));
    result->payload = malloc(sizeof(size_t) * max_num_vals);
    result->num_tuples = 0;
    result->data_type = POSITIONLIST;
    result->num_update_tuples = 0;
    // printf("unclustered select btree\n");
    // traverse_tree(root, print_node, NULL);

    BNode* left_node = root;
    while(left_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(left_node, 0, left_node->num_elements, lower, &success);
        while (idx > 0 && left_node->vals[idx] >= lower){
            idx -= 1;
        }  
        if (success == false){
            log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
            return NULL;
        }
        // printf("Child node idx %ld \n", idx );
        left_node = left_node->children.child_pointers[idx];
    }
    assert(left_node->is_leaf == true);

    bool success;
    size_t leaf_internal_idx = BNode_binary_search(left_node, 0, left_node->num_elements, lower, &success);

    // order matters here, needs to be >0 before we -- 
    while (leaf_internal_idx > 0 && left_node->vals[leaf_internal_idx - 1] >= lower){
        leaf_internal_idx -= 1;
    }

    // printf("Leaf internal idx %ld \n", leaf_internal_idx);

    // if the lower bound is in the rightmost leaf and no values in the leaf match we are done
    if (left_node->next == NULL && left_node->vals[leaf_internal_idx] < lower){
        return result;
    }

    while (left_node != NULL){
        // printf("NODE in unclustered select\n");
        // print_node(left_node, NULL);
        // if (left_node->vals[left_node->num_elements - 1] < lower){
        //     left_node = left_node->next;
        //     leaf_internal_idx = 0;
        //     continue;
        // }
        // if everything in node matches we just memcpy 
        // if (left_node->vals[left_node->num_elements - 1] < upper && left_node->vals[0] >= lower){
        //     printf("Copy full node \n");
        //     memcpy(&result->payload[result->num_tuples], &left_node->children.positions[leaf_internal_idx], (left_node->num_elements - leaf_internal_idx) * sizeof(size_t));
        //     result->num_tuples += (left_node->num_elements - leaf_internal_idx);
        //     printf("FUll copy num results %ld \n",  result->num_tuples);
        //     leaf_internal_idx = 0;
        //     left_node = left_node->next;
        // } else {
            // printf("copy less than full node for loop \n");
            // printf("Lower %d, upper %d\n", lower, upper);
           for (size_t i = leaf_internal_idx; i < left_node->num_elements; i++){
                // if (i== 19 || i==79 ){
                //     printf("i in for loo %ld\n", i);
                // }
                // printf(" val %d pred % d , pos %ld \n", left_node->vals[i],  ((left_node->vals[i] >= lower) & (left_node->vals[i] < upper)), left_node->children.positions[i] );
                ((size_t*) result->payload)[result->num_tuples] = left_node->children.positions[i];
                result->num_tuples += ((left_node->vals[i] >= lower) & (left_node->vals[i] < upper));
                // if (left_node->children.positions[i] == 0 ){
                //     printf("Pos %ld in node is 0 ", i);
                // }
            }
            // printf("after partial copy num results %ld \n",  result->num_tuples);
            leaf_internal_idx = 0;
            // } 
            // No more nodes to the left will contain values matching the predicate
            if (left_node->vals[left_node->num_elements - 1] >= upper){
                break;
            }
            left_node = left_node->next;
        // }
        
    }
    result->payload = realloc(result->payload, sizeof(size_t) * result->num_tuples);
    pos_qsort(result->payload, 0, result->num_tuples);
    // printf("before return num results %ld\n", result->num_tuples );
    return result;
}




/*
* Also needs debugging
* We probe for lower and then higher and then fill in the gaps
*/
Result* BTree_clustered_select(BNode* root, int lower, int upper){
    Result* result = malloc(sizeof(Result));
    result->data_type = POSITIONLIST;
    result->num_tuples = 0;
    result->num_update_tuples = 0;

    BNode* left_node = root;
    printf("root num elemets %ld\n", root->num_elements);
    while(left_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(left_node, 0, left_node->num_elements-1, lower, &success);
        printf("left node num_elements %ld", left_node->num_elements);
        printf(" in bnode binary search idx %ld\n", idx );
        // if (success == false){
        //     log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
        //     return result;
        // }
        left_node = left_node->children.child_pointers[idx];
    }
    assert(left_node->is_leaf == true);

    bool success;
    size_t left_start_idx = BNode_binary_search(left_node, 0, left_node->num_elements-1, lower, &success);
    // if (success == false){
    //         log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
    //         return result;
    //     }
    while (left_start_idx > 0 && left_node->vals[left_start_idx - 1] >= lower){
        left_start_idx -= 1;
    }

    size_t left_pos = left_node->children.positions[left_start_idx];



    BNode* right_node = root;
    while(right_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(right_node, 0, right_node->num_elements-1, upper, &success);
        // if (success == false){
        //     log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
        //     return result;
        // }
        right_node = right_node->children.child_pointers[idx];
    }
    assert(right_node->is_leaf == true);

    // if we have duplicate values that span across nodes
    while(right_node->next != NULL && right_node->next->vals[0] < upper){
        right_node = right_node->next;
    }

    size_t right_end_idx = BNode_binary_search(right_node, 0, right_node->num_elements-1, upper, &success);
    // if (success == false){
    //         log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
    //         return result;
    //     }
    while (right_end_idx > 0 && right_node->vals[right_end_idx] >= upper) {
        right_end_idx -= 1;
    }

    // printf("upper %d lower  %d\n", upper, lower);
    // printf("left node\n");
    // print_node(left_node, NULL);

    // printf("right node \n");
    // print_node(right_node, NULL);

    size_t right_pos = right_node->children.positions[right_end_idx];

    printf("left pos %ld  right pos %ld \n", left_pos, right_pos);
    // possilby an off by 1 here.
    result->num_tuples = right_pos - left_pos + 1;
    result->payload = malloc(sizeof(size_t) * result->num_tuples);

    size_t pos = left_pos;
    for (size_t i = 0; i < result->num_tuples; i++){
        ((size_t*) result->payload)[i] = pos;
        pos++;
    }
    
    return result;
}


int insert_into_node(BNode* node, int val, size_t pos){
    if (node->num_elements == FAN_OUT){
        return -1;
    }
    bool success;
    size_t node_idx = BNode_binary_search(node, 0, node->num_elements-1, val, &success);
    while (node_idx > 0 && node->vals[node_idx - 1] >= val){
        node_idx -= 1;
    }
    memmove(&node->vals[node_idx], &node->vals[node_idx+1], (node->num_elements - node_idx) * sizeof(int));
    memmove(&node->children.positions[node_idx], &node->children.positions[node_idx+1], (node->num_elements - node_idx) * sizeof(int));
    node->children.positions[node_idx] = pos;
    node->vals[node_idx] = val;
    node->num_elements += 1;

    for (size_t i = 0; i < node->num_elements; i++){
        if (node->vals[i] > val){
            node->children.positions[i] += 1;
        }
    }
    return 1;
}

int btree_clustered_insert(BNode* root, int val, size_t pos){
    BNode* ins_node = root;
    while(ins_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(ins_node, 0, ins_node->num_elements, val, &success);
        while (idx > 0 && ins_node->vals[idx] >= val){
            idx -= 1;
        }  
        if (success == false){
            log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
            return -1;
        }
        ins_node = ins_node->children.child_pointers[idx];
    }
    assert(ins_node->is_leaf == true);
    if (!insert_into_node(ins_node, val, pos)){
        log_err("Failed to insert into node\n");
    }

    ins_node = ins_node->next;
    while (ins_node != NULL){
        for (size_t i = 0; i < ins_node->num_elements; i++){
            if (ins_node->children.positions[i] > pos){
                ins_node->children.positions[i] += 1;
            }
            
        }
        ins_node = ins_node->next;
    }

    return 1;
}
int btree_unclustered_insert(BNode* root, int val, size_t pos){
    BNode* ins_node = root;
    while(ins_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(ins_node, 0, ins_node->num_elements, val, &success);
        while (idx > 0 && ins_node->vals[idx] >= val){
            idx -= 1;
        }  
        if (success == false){
            log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
            return -1;
        }
        ins_node = ins_node->children.child_pointers[idx];
    }
    assert(ins_node->is_leaf == true);
    if (!insert_into_node(ins_node, val, pos)){
        log_err("Failed to insert into node\n");
    }

    ins_node = ins_node->next;
    BNode* init_node = ins_node;
    while (ins_node != NULL){
        for (size_t i = 0; i < ins_node->num_elements; i++){
            if (ins_node->children.positions[i] > pos){
                ins_node->children.positions[i] += 1;
            }
            
        }
        ins_node = ins_node->next;
    }

    while (init_node != NULL){
        for (size_t i = 0; i < init_node->num_elements; i++){
            if (init_node->children.positions[i] > pos){
                init_node->children.positions[i] += 1;
            } 
        }
        init_node = init_node->previous;
    }
    return 1;
}

int btree_clustered_delete(BNode* root, int val){
    BNode* left_node = root;
    while(left_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(left_node, 0, left_node->num_elements, val, &success);
        while (idx > 0 && left_node->vals[idx] >= val){
            idx -= 1;
        }  
        if (success == false){
            log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
            return -1;
        }
        // printf("Child node idx %ld \n", idx );
        left_node = left_node->children.child_pointers[idx];
    }
    assert(left_node->is_leaf == true);

    bool success;
    size_t left_start_idx = BNode_binary_search(left_node, 0, left_node->num_elements-1, val, &success);
    while (left_start_idx > 0 && left_node->vals[left_start_idx - 1] >= val){
        left_start_idx -= 1;
    }

    memmove(&left_node->vals[left_start_idx], &left_node->vals[left_start_idx+1], (left_node->num_elements - left_start_idx) * sizeof(size_t));
    memmove(&left_node->vals[left_start_idx], &left_node->vals[left_start_idx+1], (left_node->num_elements - left_start_idx) * sizeof(int));
    left_node->num_elements -=1;

    while (left_node != NULL){
        for (size_t i = left_start_idx; i < left_node->num_elements; i++){
            left_node->children.positions[i] -= 1;
        }
        left_start_idx = 0;
        left_node = left_node->next;
    }
    return 1;
}


int btree_unclustered_delete(BNode* root, int val){
    BNode* left_node = root;
    while(left_node->is_leaf == false){
        bool success;
        size_t idx = BNode_binary_search(left_node, 0, left_node->num_elements, val, &success);
        while (idx > 0 && left_node->vals[idx] >= val){
            idx -= 1;
        }  
        if (success == false){
            log_err("%s:%d BNode binary search failed", __FILE__, __LINE__);
            return -1;
        }
        // printf("Child node idx %ld \n", idx );
        left_node = left_node->children.child_pointers[idx];
    }
    assert(left_node->is_leaf == true);

    bool success;
    size_t left_start_idx = BNode_binary_search(left_node, 0, left_node->num_elements-1, val, &success);
    while (left_start_idx > 0 && left_node->vals[left_start_idx - 1] >= val){
        left_start_idx -= 1;
    }

    size_t left_pos = left_node->children.positions[left_start_idx];

    memmove(&left_node->vals[left_start_idx], &left_node->vals[left_start_idx+1], (left_node->num_elements - left_start_idx) * sizeof(size_t));
    memmove(&left_node->vals[left_start_idx], &left_node->vals[left_start_idx+1], (left_node->num_elements - left_start_idx) * sizeof(int));
    left_node->num_elements -=1;

    BNode* init_node = left_node;
    while (left_node != NULL){
        for (size_t i = left_start_idx; i < left_node->num_elements; i++){
            if (left_node->children.positions[i] > left_pos){
                left_node->children.positions[i] -= 1;
            }
            
        }
        left_start_idx = 0;
        left_node = left_node->next;
    }

    while (init_node != NULL){
        for (size_t i = 0; i < left_node->num_elements; i++){
            if (init_node->children.positions[i] > left_pos){
                init_node->children.positions[i] -= 1;
            }            
        }
        init_node = init_node->previous;
    }
    return 1;
}