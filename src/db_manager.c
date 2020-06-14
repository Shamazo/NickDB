#include "main_api.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include "utils.h"
#include "db_persist.h"
#include "db_index.h"

// In this class, there will always be only one active database at a time
Db *g_db;

/*
* Minor refactoring note, this creates a table on the heap then copies into existing heap memory, 
* would be marginally more efficient to just put values into the existing heap structure.
*/
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
    ret_status->code = OK;
    Table* new_tb = calloc(1, sizeof(Table));
    if (new_tb==NULL){
        log_err("Failed to allocate memory for new table: %s \n", name);
        ret_status->code = ERROR;
        return NULL;
    }

    size_t dir_path_len = strlen(name) + LEN_DATA_PATH + 1;
    char* dir_path = malloc(dir_path_len * sizeof(char));
    sprintf(dir_path, "%s%s", DATA_PATH, name);
   
    if ( mkdir(dir_path, S_IRUSR | S_IWUSR) < 0){
        if (errno == EEXIST){
                ret_status->code = ERROR;
                printf("attempted data dir %s \n", dir_path );
                log_err("Table %s already exists \n", name);
                return NULL;
            } else {
                printf("attempted data dir %s \n", dir_path );
                log_err("Error making data directory: %s\n", strerror(errno));
            }
    }
    free(dir_path);
    strcpy(new_tb->name, name);
    new_tb->table_length = 0;
    new_tb->columns = calloc(num_columns, sizeof(Column));
    new_tb->col_arr_size = num_columns;
    new_tb->col_count = 0;
    new_tb->table_alloc_size = 0;
    if (new_tb->columns == NULL){
        log_err("Failed to allocate memory for columns in new table: %s \n", name);
        ret_status->code=ERROR;
        return NULL;
    }

    if (db->tables_size < db->tables_capacity){
        db->tables[db->tables_size] = *new_tb;
        db->tables_size += 1;
    } else {
        db->tables = realloc(db->tables, (db->tables_size+5)*sizeof(Table)); // 5 is arbitrary, but the idea is to do fewer reallocs
        db->tables[db->tables_size] = *new_tb;
        db->tables_size += 1;
        db->tables_capacity +=4;
    }
    free(new_tb);
    return &db->tables[db->tables_size] ;
}

/* 
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char* db_name) {
    struct Status ret_status;
    
    ret_status.code = OK;
    g_db = calloc(1, sizeof(Db));
    if (g_db == NULL){
        log_err("Failed to allocate memory for new db\n");
        ret_status.code = ERROR;
        return ret_status;
    }

    strcpy(g_db->name, db_name);
    g_db->tables = calloc(3, sizeof(Table));
    g_db->tables_size = 0;
    g_db->tables_capacity = 3;
    return ret_status;
}


/*
* Creates a column in a table. The column struct probably already exists
* as we create tables with columns, but we may have to create and realloc 
* if it does not. Additionally, for every new column we need to create a 
* new file and mmap that file. 
*/

Column* create_column(Table *table, char *name, bool sorted, Status *ret_status){
    (void) sorted;
    table->col_count +=1;
    ret_status->code = OK;
    int path_length = LEN_DATA_PATH + strlen(table->name) + strlen(name) + 4 + 2;
    char col_file_path[path_length];
    sprintf(col_file_path, "%s%s/%s%s", DATA_PATH, table->name, name, ".col");
    int fd = open(col_file_path, O_RDWR|O_CREAT, 0755);

    if (fd == -1){ 
        log_err("%s:%d Failed to open column: %s in create, errno: %d , %s \n", __FILE__, __LINE__, name, errno, strerror(errno));
        return false;
    } 

    if(table->col_count >= table->col_arr_size){
        table->columns = realloc(table->columns, (table->col_arr_size+5) * sizeof(Column));
        table->col_arr_size += 5; 
    }

    strcpy(table->columns[table->col_count-1].name, name);
    // if inserting column into existing table with data then we want it to be the correct size. 
    size_t table_len;
    if (table->table_length > 1000){
        table_len = table->table_length;
    } else {
        table_len = 1000;
    }
    ftruncate(fd, table_len * sizeof(int));
    // only support int data to begin with
    table->columns[table->col_count-1].data = mmap(NULL, table_len * sizeof(int), PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);
    if (table->columns[table->col_count-1].data == NULL){
        log_err("%s:%d, Failed to mmap column, errno: %d , strerror: %s \n", __FILE__, __LINE__, errno, strerror(errno));
        ret_status->code = ERROR;
        return NULL;
    }
    table->columns[table->col_count-1].num_rows = &table->table_length;
    table->columns[table->col_count-1].index_type = NONE;
    table->columns[table->col_count-1].clustered = false;

    table->columns[table->col_count-1].update_struct.alloc_size = UPDATE_BATCH_SIZE;
    table->columns[table->col_count-1].update_struct.del_pos = malloc(sizeof(size_t) * UPDATE_BATCH_SIZE);
    table->columns[table->col_count-1].update_struct.del_val = malloc(sizeof(int) * UPDATE_BATCH_SIZE);
    table->columns[table->col_count-1].update_struct.ins_val = malloc(sizeof(int) * UPDATE_BATCH_SIZE);
    table->columns[table->col_count-1].update_struct.del_length = 0;
    table->columns[table->col_count-1].update_struct.ins_length = 0;
    table->table_alloc_size = table_len;

    close(fd);
    return &table->columns[table->col_count-1];
}


/*
* Deletes on base data also cover deletes on clustered sorted indexes
*/
bool flush_deletes(Table* table){
    // delete from base data first
    size_t init_num_rows = table->table_length;
    for (size_t i=0; i < table->col_count; i++){
        size_t local_num_rows = init_num_rows;
        for (size_t j; j < table->columns[i].update_struct.del_length; j++){
            memmove(&table->columns[i].data[table->columns[i].update_struct.del_pos[j]], 
                &table->columns[i].data[table->columns[i].update_struct.del_pos[j]+1],
                (local_num_rows - table->columns[i].update_struct.del_length) * sizeof(int));
            local_num_rows -= 1;          
            // update any indexes
            if (table->columns[i].index_type == SORTED && table->columns[i].clustered == true){
                sorted_clustered_delete((SortedIndex*)table->columns[i].index, table->columns[i].update_struct.del_val[j]);
            } else if (table->columns[i].index_type == SORTED && table->columns[i].clustered == false){
                sorted_unclustered_delete((SortedIndex*)table->columns[i].index, table->columns[i].update_struct.del_val[j]);
            } else if (table->columns[i].index_type == BTREE && table->columns[i].clustered == true){
                btree_clustered_delete((BNode*)table->columns[i].index, table->columns[i].update_struct.del_val[j]);
            } else if (table->columns[i].index_type == BTREE && table->columns[i].clustered == false){
                btree_unclustered_delete((BNode*) table->columns[i].index, table->columns[i].update_struct.del_val[j]);
            }
        }
    }
    table->table_length -= table->columns[0].update_struct.del_length;

    for (size_t i=0; i < table->col_count; i++){
        table->columns[i].update_struct.del_length = 0;
    }

    return true;
}

bool flush_inserts(Table* table){
    Column* clustered_col = NULL;
    for (size_t i=0; i < table->col_count; i++){
        if (table->columns[i].clustered == true){
            clustered_col = &table->columns[i];
        }
    }

    // no primary indexes means we can just append data
    if (clustered_col == NULL){
        for (size_t j=0; j < table->columns[0].update_struct.ins_length; j++){
            size_t ins_pos = table->table_length;
            for (size_t i=0; i < table->col_count; i++){
                table->columns[i].data[ins_pos] = table->columns[i].update_struct.ins_val[j];

                // update any indexes
                if (table->columns[i].index_type == SORTED && table->columns[i].clustered == true){
                    sorted_clustered_insert((SortedIndex*)table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                } else if (table->columns[i].index_type == SORTED && table->columns[i].clustered == false){
                    sorted_unclustered_insert((SortedIndex*)table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                } else if (table->columns[i].index_type == BTREE && table->columns[i].clustered == true){
                    btree_clustered_insert((BNode*)table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                } else if (table->columns[i].index_type == BTREE && table->columns[i].clustered == false){
                    btree_unclustered_insert((BNode*) table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                }
            }
            table->table_length += 1;
        }   
    } else {
        for (size_t j=0; j <  table->columns[0].update_struct.ins_length; j++){
            size_t ins_pos = clustered_insert_position(clustered_col->data, 0, table->table_length, clustered_col->update_struct.ins_val[j]);
            for (size_t i=0; i < table->col_count; i++){
                memmove(&table->columns[i].data[ins_pos], &table->columns[i].data[ins_pos + 1],
                (table->table_length - ins_pos) * sizeof(int));
                table->columns[i].data[ins_pos] = table->columns[i].update_struct.ins_val[j];

                // update any indexes
                if (table->columns[i].index_type == SORTED && table->columns[i].clustered == true){
                    sorted_clustered_insert((SortedIndex*)table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                } else if (table->columns[i].index_type == SORTED && table->columns[i].clustered == false){
                    sorted_unclustered_insert((SortedIndex*)table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                } else if (table->columns[i].index_type == BTREE && table->columns[i].clustered == true){
                    btree_clustered_insert((BNode*)table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                } else if (table->columns[i].index_type == BTREE && table->columns[i].clustered == false){
                    btree_unclustered_insert((BNode*) table->columns[i].index, table->columns[i].update_struct.ins_val[j], ins_pos);
                }
            }
            table->table_length += 1;   
        }         
    }
    for (size_t i=0; i < table->col_count; i++){
        table->columns[i].update_struct.ins_length = 0;
    }
    return true;
}

/*
* Flush updates on table to underlying data and clear differential struct for each column
*/
bool flush_updates(Table* table){
    printf("FLUSHING UPDATES \n");

    if (table->table_length + table->columns[0].update_struct.ins_length > table->table_alloc_size){
        resize_table(table, table->table_length + table->columns[0].update_struct.ins_length);
    }

    flush_deletes(table);
    flush_inserts(table);
    return true;
}

/*
*
*/
bool free_update_structure(Table* table){
    for (size_t i = 0; i < table->col_count; i++){
        Column* curr_column = &table->columns[i];
        free(curr_column->update_struct.del_pos);
        free(curr_column->update_struct.del_val);
        free(curr_column->update_struct.ins_val);
    }

    return true;
}


bool resize_column(Table* table, Column* column, size_t new_size){
    if (!save_column_data(column, table->table_alloc_size)){
        log_err("%s:%d Failed to save column: %s before resizing\n", __FILE__, __LINE__, column->name);
        return false;
    }
    int path_length = LEN_DATA_PATH + strlen(table->name) + strlen(column->name) + 4 + 2;
    char col_file_path[path_length];
    sprintf(col_file_path, "%s%s/%s%s", DATA_PATH, table->name, column->name, ".col");
    int fd = open(col_file_path, O_RDWR);

    if (fd == -1){ 
        log_err("%s:%d Failed to open column: %s in resize, errno: %d , %s \n", __FILE__, __LINE__, table->name, errno, strerror(errno));
        return false;
    } 

    ftruncate(fd, new_size * sizeof(int));
    // only support int data to begin with
    column->data = mmap(NULL, new_size * sizeof(int), PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);
    if (table->columns[table->col_count-1].data == NULL){
        log_err("%s:%d, Failed to mmap column, errno: %d , strerror: %s \n", __FILE__, __LINE__, errno, strerror(errno));
        return false;
    }

    if (column->index_type == SORTED && column->clustered){
        ((SortedIndex*) column->index)->keys = column->data;
    }

    close(fd);
    return true;
}

bool resize_table(Table* table, size_t new_size){
    bool ret = true;
    for (size_t i=0; i < table->col_count; i++){
        if(!resize_column(table, &(table->columns[i]), new_size)){
            ret = false;
        }
    }
    table->table_alloc_size = new_size;
    return ret;
}