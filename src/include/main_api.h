


/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef MAIN_H
#define MAIN_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64

#define NUM_THREADS 60
#define QUERIES_PER_THREAD 5
#define MULTI_THEADING 1
#define SS_VECTOR_SIZE 1000000

#define INDEXES 0

#define TLB 16
#define JOINTHREADS 8

#define UPDATE_BATCH_SIZE  15000


/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types. BICVECTOR not currently used, but could be one way 
 * to implement bitvectors as results without a new struct 
 * position lists uses size_t type
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT,
     POSITIONLIST,
     BITVECTOR,
     DOUBLE
} DataType;

typedef enum IndexType {
    NONE,
    BTREE,
    SORTED
} IndexType;



typedef struct DiffUpdate {
    size_t alloc_size;
    size_t* del_pos;
    int* del_val;
    int* ins_val;
    size_t del_length;
    size_t ins_length;
} DiffUpdate;


struct Comparator;
//struct ColumnIndex;
struct Table;
typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;
    DataType INT;
    // You will implement column indexes later. 
    void* index;
    size_t *num_rows;
    IndexType index_type;
    DiffUpdate update_struct;
    //struct ColumnIndex *index;
    bool clustered;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of active columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table. i.e number of tuples in table
 * - table_alloc_size, the amount of space in each column in terms of entries NOT bytes
 * - col_arr_size the size of columns in terms of sizeof(Column)
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
    size_t table_alloc_size;
    size_t col_arr_size;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the the number of table in the array
 * - tables_capacity: the amount of tables that can be held in the currently allocated memory slot 
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;


/*
* WARNING GLOBAL
* This is a pointer to the database struct
* in this project we only support a single database, if adding support for multiple
* this would probably be a pointer to a global catalogue struct. 
* It is instantiated in server.c
*/

extern Db* g_db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    size_t num_update_tuples;
    DataType data_type;
    void *payload;
} Result;


/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */

struct DbOperator;
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    size_t chandles_in_use;
    size_t chandle_slots;
    bool batching_active;
    bool incoming_load;
    struct DbOperator* batch_operators;
    size_t batch_operator_slots;
    size_t batch_operators_in_use;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 * Operators pass around columns and variables as strings
 * since in the future there may be multiple physical columns for a single column name
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    SHUTDOWN,
    SELECT,
    FETCH,
    PRINT,
    PRINT_INDEX,
    LOAD,
    AVG,
    SUM,
    MAX,
    MIN,
    ADD,
    SUB,
    BATCH_QUERIES,
    BATCH_EXECUTE,
    JOIN,
    UPDATE,
    DELETE
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
    _INDEX
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    int col_count;
    IndexType index_type;
    Column* column;
    bool clustered;
} CreateOperator;


/*
 * neccessary fields for selecting. 
 * src - column or intermediate result we are selecting from
 * indices - position vector on the source, optional 
 * handle - where to store result in client context, stored as char* since
 * we handle it differently depending on whether it exists in current context or not
 */
typedef struct SelectOperator{
    // Table* table;
    GeneralizedColumn* src;
    Result* indices;
    char handle[MAX_SIZE_NAME];
    int minimum;
    int maximum;
    bool use_index_vector;
} SelectOperator;

/*
* Neccessary fields to print
* length is # rows
* count is # columns
* column is used for printing an index for debugging
*/
typedef struct PrintOperator
{
    GeneralizedColumn** columns;
    size_t col_count;
    size_t col_length;
    Column* column;
} PrintOperator;


/*
 * necessary fields for Fetching
 */
typedef struct FetchOperator {
    Column* column;
    char* handle;
    Result* indices;
} FetchOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;


typedef struct UpdateOperator {
    Column* column;
    Result* positions;
    int value;
} UpdateOperator;

typedef struct DeleteOperator {
    Table* table;
    Result* positions;
} DeleteOperator;

/*
 * necessary fields for Loading. 
 * values is an array of arrays. Each inner array corresponds to a column
 */
typedef struct LoadOperator {
    Table* table;
    int** values;
    size_t num_cols;
    size_t num_rows;
} LoadOperator;


/*
* Operator fields for avg, sum, min, max
*/

typedef struct AggOperator {
    char handle[MAX_SIZE_NAME];
    GeneralizedColumn column;
} AggOperator;

/*
* Operator fields for add, sub
*/
typedef struct BinaryOperator {
    char handle[MAX_SIZE_NAME];
    GeneralizedColumn left_column;
    GeneralizedColumn right_column;
} BinaryOperator;



typedef enum JoinType {
    HASH,
    NESTEDLOOP
} JoinType;

typedef struct JoinOperator{
    char handle_left[MAX_SIZE_NAME];
    char handle_right[MAX_SIZE_NAME];
    GeneralizedColumn* left_val;
    GeneralizedColumn* left_pos;
    GeneralizedColumn* right_val;
    GeneralizedColumn* right_pos;
    JoinType join_type;
} JoinOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    LoadOperator load_operator;
    AggOperator agg_operator;
    BinaryOperator binary_operator;
    JoinOperator join_operator;
    UpdateOperator update_operator;
    DeleteOperator delete_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;


/*
* SharedScanArgs is used to pass arguments to a execute_shared_select, which is used for multi threading
* comps is the array of compariasons, one for each query
* data is a a pointer to the start of the vector to scan 
* data_type is used to cast the data to the correct type for pointer arithmetic
* vect_size is the how far to read into data
* results is an array of Result pointers, one fo reach query. Each result is appended to. 
*/

typedef struct SharedScanArgs {
    size_t num_queries;
    Comparator comps[QUERIES_PER_THREAD];
    void* data;
    DataType data_type;
    size_t vect_size;
    size_t position_offset;
    Result* results[QUERIES_PER_THREAD] ;
} SharedScanArgs;



typedef struct JoinPartitions {
    size_t** left_pos_partitions;
    size_t** right_pos_partitions;
    int** left_val_partitions;
    int** right_val_partitions;

    size_t* left_partition_lengths;
    size_t* right_partition_lengths;
    size_t num_parts;
} JoinPartitions;


typedef struct PartitionThreadArgs {
    size_t* positions;
    int* vals;
    size_t** pos_partitions;
    int** val_partitions;
    size_t* partition_lengths;
    size_t num_parts;
    size_t data_length;
} PartitionThreadArgs;


typedef struct HashJoinThreadArgs {
    size_t* left_pos;
    int* left_vals;
    size_t* right_pos;
    int* right_vals;
    size_t* left_join_positions;
    size_t* right_join_positions;
    size_t left_len;
    size_t right_len;
    size_t join_len;

} HashJoinThreadArgs;







extern Db *current_db;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

Status create_db(const char* db_name);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

Column* create_column(Table *table, char *name, bool sorted, Status *ret_status);

//Should these be here or db_persist.h?

bool resize_column(Table* table, Column* column, size_t new_size);
bool resize_table(Table* table, size_t new_size);

Status shutdown_server();

char* execute_DbOperator(DbOperator* query, ClientContext* client_context);
void db_operator_free(DbOperator* query);
bool flush_updates(Table* table);
bool free_update_structure(Table* table);


#endif /* MAIN_H */

