
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <assert.h>
#include <signal.h>
#include "client_context.h"
#include "db_persist.h"
#include "db_index.h"
#include "common.h"
#include "parse.h"
#include "main_api.h"
#include "message.h"
#include "utils.h"
#include "db_hashtable.h"


char* execute_print(DbOperator* query) {
    // currently only supporting int data.
    // 12 = 11 characters per max_int/min_int including sign + 1 for space after each last space is \n instead
    char* return_msg = malloc(12 * query->operator_fields.print_operator.col_count * query->operator_fields.print_operator.col_length * sizeof(char));
    return_msg[0] = '\0';
    int return_msg_pos = 0;
    char* row = malloc(12 * query->operator_fields.print_operator.col_count * sizeof(char));
    char* ascii_data = malloc(12 * sizeof(char));
    // printf("Col length in query %ld\n", query->operator_fields.print_operator.col_length);
    for (size_t i=0; i < query->operator_fields.print_operator.col_length; i++){
        
        size_t row_pos = 0;
        for (size_t j=0; j < query->operator_fields.print_operator.col_count; j++){
            
            if (query->operator_fields.print_operator.columns[j]->column_type == COLUMN){
                int data = query->operator_fields.print_operator.columns[j]->column_pointer.column->data[i];
                sprintf(ascii_data, "%d", data);
            } else {
                if (query->operator_fields.print_operator.columns[j]->column_pointer.result->payload == NULL ){
                    ;
                } else if (query->operator_fields.print_operator.columns[j]->column_pointer.result->data_type == INT){
                    int* data_pointer = (int *) query->operator_fields.print_operator.columns[j]->column_pointer.result->payload;
                    int data =  data_pointer[i];
                    sprintf(ascii_data, "%d", data);
                } else if (query->operator_fields.print_operator.columns[j]->column_pointer.result->data_type == LONG){
                    long* data_pointer = (long *) query->operator_fields.print_operator.columns[j]->column_pointer.result->payload;
                    long data =  data_pointer[i];
                    sprintf(ascii_data, "%ld", data);
                } else if (query->operator_fields.print_operator.columns[j]->column_pointer.result->data_type == POSITIONLIST){
                    size_t* data_pointer = (size_t *) query->operator_fields.print_operator.columns[j]->column_pointer.result->payload;
                    size_t data =  data_pointer[i];
                    sprintf(ascii_data, "%ld", data);
                } else if (query->operator_fields.print_operator.columns[j]->column_pointer.result->data_type == FLOAT){
                    float* data_pointer = (float *) query->operator_fields.print_operator.columns[j]->column_pointer.result->payload;
                    float data =  data_pointer[i];
                    sprintf(ascii_data, "%.2f", data);
                } else if (query->operator_fields.print_operator.columns[j]->column_pointer.result->data_type == DOUBLE){
                    double* data_pointer = (double *) query->operator_fields.print_operator.columns[j]->column_pointer.result->payload;
                    double data =  data_pointer[i];
                    sprintf(ascii_data, "%.2lf", data);
                }
            }
            strcpy(&row[row_pos], ascii_data);
            row_pos += strlen(ascii_data);
            row[row_pos] = ',';
            row_pos += 1;
            
        }
        row[row_pos-1] = '\n';
        row[row_pos] = '\0';
        strcpy(&return_msg[return_msg_pos], row);
        return_msg_pos += strlen(row);
    }
    return_msg[return_msg_pos + 1] = '\0';
    free(ascii_data); 
    free(row);
    for (size_t j=0; j < query->operator_fields.print_operator.col_count; j++){
        if (query->operator_fields.print_operator.columns[j]->column_type == COLUMN){
            free(query->operator_fields.print_operator.columns[j]);
        }
    }
    free(query->operator_fields.print_operator.columns);

    return return_msg;
}


char* execute_print_index(DbOperator* query) {

    Column* col = query->operator_fields.print_operator.column;
    char* ret;
    if (col->index_type == NONE){
        return "No Index";
    } else if (col->index_type == BTREE){
        ret = malloc(sizeof(char) * (*col->num_rows) * 13 * 2);
        BNode* curr_node = (BNode*) col->index;
        while (curr_node->is_leaf == false){
            curr_node = curr_node->children.child_pointers[0];
        }
        size_t j = 0;
        size_t num_nodes = 0;
        while (curr_node != NULL){
            for (size_t i =0; i < curr_node->num_elements; i++){
                j += sprintf(ret + j, "%d\n", curr_node->vals[i]);
            }
            num_nodes += 1;
            curr_node = curr_node->next;
        }
    }

    return ret;


}



char* execute_load(DbOperator* query) {
    //If our columns are too small then we need to increase the size of the column
    Table* table = query->operator_fields.load_operator.table;
    printf("Loading into table %s \n", table->name);



    size_t new_size = 0;
    if(table->table_alloc_size < (table->table_length + query->operator_fields.load_operator.num_rows)){
        new_size = 2 * (table->table_length + query->operator_fields.load_operator.num_rows);
        resize_table(table, new_size);
    }
    const size_t num_cols = query->operator_fields.load_operator.num_cols;
    printf("load num rows %ld \n", query->operator_fields.load_operator.num_rows );
    printf("load num cols %ld \n", num_cols);
    for (size_t i=0; i < num_cols; i++){
        memcpy(&table->columns[i].data[table->table_length], 
                query->operator_fields.load_operator.values[i], 
                query->operator_fields.load_operator.num_rows * sizeof(int)); 
        free(query->operator_fields.load_operator.values[i]);
    }
    table->table_length += query->operator_fields.load_operator.num_rows;

    free(query->operator_fields.load_operator.values);


    if(INDEXES ){
        // could pretty easily spin up a thread for each unclustered index. 
        // want to make clustered indices first, also assuming at most one clustered index per table
        for (size_t i=0; i < num_cols; i++){
            if (table->columns[i].index_type != NONE){
                if (table->columns[i].clustered == true){
                    if (table->columns[i].index_type == SORTED){
                        load_into_clustered_sorted_index(table, &table->columns[i]);
                    } else if (table->columns[i].index_type == BTREE){
                        load_into_clustered_btree_index(table, &table->columns[i]);
                    } 
                } 
            }
        }


        // once clustered indices are created we build the unclustered indices.
        for (size_t i=0; i < num_cols; i++){
            if (table->columns[i].index_type != NONE){
                if (table->columns[i].clustered == false){
                    if (table->columns[i].index_type == SORTED){
                        load_into_unclustered_sorted_index(&table->columns[i]);
                    } else if (table->columns[i].index_type == BTREE){
                        load_into_unclustered_btree_index(&table->columns[i]);
                    }
                    
                }
            }
        }
    }
    
    return " ";

}


Result* adjust_result_for_updates(Result* result, Column* column, int min_val, int max_val){
    size_t* new_payload = malloc(sizeof(size_t) * (result->num_tuples + column->update_struct.ins_length));
    size_t j = 0;
    size_t increment = 0;
    // iterate through results, skipping over those which have been deleted 
    for (size_t i=0; i < result->num_tuples; i++){
        new_payload[j] = ((size_t*) result->payload)[i];

        increment = 1;
        for (size_t k = 0; k < column->update_struct.del_length; k++){
            if (((size_t*) result->payload)[i] == column->update_struct.del_pos[k]){
                increment = 0;
                break;
            } 
        }
        j += increment;     
    }

    // add anything which matches the predicate 
    result->num_update_tuples = 0;
    for (size_t k = 0; k < column->update_struct.ins_length; k++){
        new_payload[j] =  k + *column->num_rows;
        j += (column->update_struct.ins_val[k] >= min_val) & (column->update_struct.ins_val[k] < max_val);
        result->num_update_tuples += (column->update_struct.ins_val[k] >= min_val) & (column->update_struct.ins_val[k] < max_val);
    }
    new_payload = realloc(new_payload, j * sizeof(size_t));
    free(result->payload);
    result->payload = new_payload;
    result->num_tuples = j;
    return result;
}

Result* execute_scan_select(DbOperator* query, const int min_val, const int max_val){
    //src is column or vector of values, rather than indices 
    size_t num_rows;
    int* src = NULL;
    if (query->operator_fields.select_operator.src->column_type == COLUMN){
        printf("Selecting from column\n" );
        src = query->operator_fields.select_operator.src->column_pointer.column->data;
        num_rows = *(query->operator_fields.select_operator.src->column_pointer.column->num_rows);
    } else {
        printf("selecting from result \n");
        // still only dealing with ints 
        if (query->operator_fields.select_operator.src->column_pointer.result->data_type != INT){
            return NULL;
        }
        src = (int*) query->operator_fields.select_operator.src->column_pointer.result->payload;
        num_rows = query->operator_fields.select_operator.src->column_pointer.result->num_tuples;
    }

    const size_t cnum_rows = num_rows;
    size_t* payload = malloc(num_rows * sizeof(size_t));
    size_t j = 0;
    if (!query->operator_fields.select_operator.use_index_vector){
        for (size_t i=0; i < cnum_rows; i++){
            payload[j] = i;
            j += ((src[i] >= min_val) & (src[i] < max_val));
        }
    } else {
        if (query->operator_fields.select_operator.indices->data_type != POSITIONLIST){
            log_err("Selects with indices currently only supports position lists\n");
            return NULL;
        }

        size_t* pos_vect = (size_t*) query->operator_fields.select_operator.indices->payload;
        for (size_t i=0; i < cnum_rows; i++){
            payload[j] = pos_vect[i];
            // printf(" src: %d, min %d, max %d,  \n", src[i], query->operator_fields.select_operator.minimum, query->operator_fields.select_operator.maximum );
            j += ((src[i] >= min_val) & (src[i] < max_val));
        }
    }
    

    Result* result = calloc(1, sizeof(Result));
    result->num_tuples = j; 
    // log_err("j is after select: %ld", j);
    result->data_type = POSITIONLIST;
    result->payload = payload;    
    return result;
}


char* execute_select(DbOperator* query, ClientContext* client_context){
    Column* column = query->operator_fields.select_operator.src->column_pointer.column;
    Result* result;
    size_t num_rows;
    const int min_val = query->operator_fields.select_operator.minimum;
    const int max_val = query->operator_fields.select_operator.maximum; 

    //todo, actually decide when to scan and when to use index on column
    if (query->operator_fields.select_operator.src->column_type == COLUMN && INDEXES){
        num_rows = *(query->operator_fields.select_operator.src->column_pointer.column->num_rows);


        switch(column->index_type) {
            case NONE:
                result = execute_scan_select(query, min_val, max_val);
                break;

            case BTREE:
                if (column->clustered == true){
                    result = BTree_clustered_select(column->index, min_val, max_val);
                } else {
                    // if (max_val - min_val < 100){
                        result = BTree_unclustered_select(column->index, num_rows, min_val, max_val);
                    // } else {
                    //     result = execute_scan_select(query, min_val, max_val);
                    // }
                }
                break;

            case SORTED:
                if (column->clustered == true){
                    result = sorted_range_select(column->index, min_val, max_val);
                    // result = execute_scan_select(query, client_context, min_val, max_val);
                } else {
                    // if (max_val - min_val < 100){
                     result = sorted_range_select(column->index, min_val, max_val);
                    // } else {
                    //     result = execute_scan_select(query, min_val, max_val);
                    // }   
                }
                break;         
        }

    // only adjust results for queries on columns
    
    } else {
        // no indexes on results, just scan them
        result = execute_scan_select(query, min_val, max_val);
    }

    assert(result != NULL);
    adjust_result_for_updates(result, column, min_val, max_val);
    insert_result_context(result, query->operator_fields.select_operator.handle, client_context);
    // if src is a column then we malloced a generalized column type while parsing
    if (query->operator_fields.select_operator.src->column_type == COLUMN){
        free(query->operator_fields.select_operator.src);
    }
    return "";
}


char* execute_fetch(DbOperator* query, ClientContext* client_context){
    if(query->operator_fields.fetch_operator.indices->data_type != POSITIONLIST){
        return "error: indices are not position list";
    }
    Result* result = malloc(sizeof(Result));
    int* payload;
    size_t j= 0;
    if (query->operator_fields.fetch_operator.indices->num_tuples == 0){
        payload = NULL;
    } else {
        payload = malloc(query->operator_fields.fetch_operator.indices->num_tuples * sizeof(int));
        size_t* indices = query->operator_fields.fetch_operator.indices->payload;
        for (size_t i=0; i < query->operator_fields.fetch_operator.indices->num_tuples - query->operator_fields.fetch_operator.indices->num_update_tuples; i++){
            payload[j++] = query->operator_fields.fetch_operator.column->data[indices[i]];
        }

        // go through the differential struct where has positions starting from column.num_rows
        for (size_t i = query->operator_fields.fetch_operator.indices->num_tuples - query->operator_fields.fetch_operator.indices->num_update_tuples; 
                i < query->operator_fields.fetch_operator.indices->num_tuples; i++){
            payload[j++] = query->operator_fields.fetch_operator.column->update_struct.ins_val[indices[i] % *query->operator_fields.fetch_operator.column->num_rows];
        }
    }

    
    result->num_tuples = j;
    result->data_type = INT;
    result->payload = payload;

    // printf("fetch num tuples %ld, handle %s\n", j, query->operator_fields.fetch_operator.handle );

    insert_result_context(result, query->operator_fields.fetch_operator.handle, client_context);
    return " ";
}


char* execute_avg(DbOperator* query, ClientContext* client_context){
    // still only handling ints 
    if (query->operator_fields.agg_operator.column.column_type == RESULT &&
            query->operator_fields.agg_operator.column.column_pointer.result->data_type != INT){
        return "Attempted to avg non-int column/result";
    }
    double total = 0.0;
    double* payload = malloc(sizeof(double));
    if (query->operator_fields.agg_operator.column.column_type == RESULT){
        if (query->operator_fields.agg_operator.column.column_pointer.result->num_tuples != 0){
                for (size_t i=0; i < query->operator_fields.agg_operator.column.column_pointer.result->num_tuples; i++){
                total += ((int*) query->operator_fields.agg_operator.column.column_pointer.result->payload)[i];
            }
            *payload = total / query->operator_fields.agg_operator.column.column_pointer.result->num_tuples;
        }    
    } else {
        if (*(query->operator_fields.agg_operator.column.column_pointer.column->num_rows) != 0){
            for (size_t i=0; i < *(query->operator_fields.agg_operator.column.column_pointer.column->num_rows); i++){
                total += query->operator_fields.agg_operator.column.column_pointer.column->data[i];   
            }
            *payload = total / (double)*(query->operator_fields.agg_operator.column.column_pointer.column->num_rows);  
        }
    }

    Result* result = malloc(sizeof(Result));
    result->num_tuples = 1;
    result->data_type = DOUBLE;
    result->payload = payload;
    insert_result_context(result, query->operator_fields.agg_operator.handle, client_context);
    return " ";
}


char* execute_sum(DbOperator* query, ClientContext* client_context){
    // still only handling ints 
    if (query->operator_fields.agg_operator.column.column_type == RESULT &&
            query->operator_fields.agg_operator.column.column_pointer.result->data_type != INT){
        return "Attempted to sum a non-int column/result";
    }
    long total = 0;
    long* payload = malloc(sizeof(long));
    if (query->operator_fields.agg_operator.column.column_type == RESULT){
        for (size_t i=0; i < query->operator_fields.agg_operator.column.column_pointer.result->num_tuples; i++){
            total += ((int*)query->operator_fields.agg_operator.column.column_pointer.result->payload)[i]; 
        }
        *payload = total;
    } else {
        for (size_t i=0; i < *(query->operator_fields.agg_operator.column.column_pointer.column->num_rows); i++){
            total += query->operator_fields.agg_operator.column.column_pointer.column->data[i];
        }
        *payload = total;
    }

    Result* result = malloc(sizeof(Result));
    result->num_tuples = 1;
    result->data_type = LONG;
    result->payload = payload;
    printf(" long sum is %ld \n", *payload);
    insert_result_context(result, query->operator_fields.agg_operator.handle, client_context);
    return " ";
}


char* execute_max(DbOperator* query, ClientContext* client_context){
    // still only handling ints 
    if (query->operator_fields.agg_operator.column.column_type == RESULT &&
            query->operator_fields.agg_operator.column.column_pointer.result->data_type != INT){
        return "Attempted to max a non-int column/result";
    }
    int max = INT_MIN;
    int* payload = malloc(sizeof(float));
    if (query->operator_fields.agg_operator.column.column_type == RESULT){
        for (size_t i=0; i < query->operator_fields.agg_operator.column.column_pointer.result->num_tuples; i++){
            max = ((int*)query->operator_fields.agg_operator.column.column_pointer.result->payload)[i] > max ?
                     ((int*)query->operator_fields.agg_operator.column.column_pointer.result->payload)[i] : max;
        }
        *payload = max;
    } else {
        for (size_t i=0; i < *(query->operator_fields.agg_operator.column.column_pointer.column->num_rows); i++){
            max = query->operator_fields.agg_operator.column.column_pointer.column->data[i] > max ?
                    query->operator_fields.agg_operator.column.column_pointer.column->data[i] : max;
        }
        *payload = max;
    }

    Result* result = malloc(sizeof(Result));
    result->num_tuples = 1;
    result->data_type = INT;
    result->payload = payload;
    insert_result_context(result, query->operator_fields.agg_operator.handle, client_context);
    return " ";
}


char* execute_min(DbOperator* query, ClientContext* client_context){
    // still only handling ints 
    if (query->operator_fields.agg_operator.column.column_type == RESULT &&
            query->operator_fields.agg_operator.column.column_pointer.result->data_type != INT){
        return "Attempted to max a non-int column/result";
    }
    int min = INT_MAX;
    int* payload = malloc(sizeof(float));
    if (query->operator_fields.agg_operator.column.column_type == RESULT){
        for (size_t i=0; i < query->operator_fields.agg_operator.column.column_pointer.result->num_tuples; i++){
            min = ((int*)query->operator_fields.agg_operator.column.column_pointer.result->payload)[i] < min ?
                     ((int*)query->operator_fields.agg_operator.column.column_pointer.result->payload)[i] : min;
        }
        *payload = min;
    } else {
        for (size_t i=0; i < *(query->operator_fields.agg_operator.column.column_pointer.column->num_rows); i++){
            min = query->operator_fields.agg_operator.column.column_pointer.column->data[i] < min ?
                    query->operator_fields.agg_operator.column.column_pointer.column->data[i] : min;
        }
        *payload = min;
    }

    Result* result = malloc(sizeof(Result));
    result->num_tuples = 1;
    result->data_type = INT;
    result->payload = payload;
    insert_result_context(result, query->operator_fields.agg_operator.handle, client_context);
    return " ";
}


char* execute_add(DbOperator* query, ClientContext* client_context){
    // still only handling ints 
    if ( (query->operator_fields.binary_operator.left_column.column_type == RESULT &&
            query->operator_fields.binary_operator.left_column.column_pointer.result->data_type != INT) ||
            (query->operator_fields.binary_operator.right_column.column_type == RESULT &&
            query->operator_fields.binary_operator.right_column.column_pointer.result->data_type != INT) ){
        return "Attempted to add a non-int column/result";
    }

    int* payload = malloc(sizeof(int) * query->operator_fields.binary_operator.left_column.column_pointer.result->num_tuples);

    // Logic to see what combination of.left_column and.right_column are column and result type and then operate on them accordingly
    if (query->operator_fields.binary_operator.left_column.column_type == RESULT && query->operator_fields.binary_operator.right_column.column_type == RESULT){
        for (size_t i=0; i < query->operator_fields.binary_operator.left_column.column_pointer.result->num_tuples; i++){
            payload[i] = ((int*)query->operator_fields.binary_operator.left_column.column_pointer.result->payload)[i] +
                     ((int*)query->operator_fields.binary_operator.right_column.column_pointer.result->payload)[i]; 
        }
    } else if (query->operator_fields.binary_operator.left_column.column_type == RESULT && query->operator_fields.binary_operator.right_column.column_type == COLUMN) {
        for (size_t i=0; i < *(query->operator_fields.binary_operator.left_column.column_pointer.column->num_rows); i++){
            payload[i] = ((int*)query->operator_fields.binary_operator.left_column.column_pointer.result->payload)[i] +
                    query->operator_fields.binary_operator.right_column.column_pointer.column->data[i];
        }
    } else if (query->operator_fields.binary_operator.left_column.column_type == COLUMN && query->operator_fields.binary_operator.right_column.column_type == RESULT) {
        for (size_t i=0; i < *(query->operator_fields.binary_operator.left_column.column_pointer.column->num_rows); i++){
            payload[i] = query->operator_fields.binary_operator.left_column.column_pointer.column->data[i] +
                    ((int*)query->operator_fields.binary_operator.right_column.column_pointer.result->payload)[i];
        }
    } else {
        for (size_t i=0; i < *(query->operator_fields.binary_operator.left_column.column_pointer.column->num_rows); i++){
            payload[i] = query->operator_fields.binary_operator.left_column.column_pointer.column->data[i] +
                    query->operator_fields.binary_operator.right_column.column_pointer.column->data[i];
        }
    }

    Result* result = malloc(sizeof(Result));
    result->num_tuples = query->operator_fields.binary_operator.left_column.column_pointer.result->num_tuples;
    result->data_type = INT;
    result->payload = payload;
    insert_result_context(result, query->operator_fields.binary_operator.handle, client_context);
    return " ";
}


char* execute_sub(DbOperator* query, ClientContext* client_context){
    // still only handling ints 
    if ( (query->operator_fields.binary_operator.left_column.column_type == RESULT &&
            query->operator_fields.binary_operator.left_column.column_pointer.result->data_type != INT) ||
            (query->operator_fields.binary_operator.right_column.column_type == RESULT &&
            query->operator_fields.binary_operator.right_column.column_pointer.result->data_type != INT) ){
        return "Attempted to add a non-int column/result";
    }

    int* payload = malloc(sizeof(int) * query->operator_fields.binary_operator.left_column.column_pointer.result->num_tuples);

    // Logic to see what combination of.left_column and.right_column are column and result type and then operate on them accordingly
    if (query->operator_fields.binary_operator.left_column.column_type == RESULT && query->operator_fields.binary_operator.right_column.column_type == RESULT){
        for (size_t i=0; i < query->operator_fields.binary_operator.left_column.column_pointer.result->num_tuples; i++){
            payload[i] = ((int*)query->operator_fields.binary_operator.left_column.column_pointer.result->payload)[i] -
                     ((int*)query->operator_fields.binary_operator.right_column.column_pointer.result->payload)[i]; 
        }
    } else if (query->operator_fields.binary_operator.left_column.column_type == RESULT && query->operator_fields.binary_operator.right_column.column_type == COLUMN) {
        for (size_t i=0; i < *(query->operator_fields.binary_operator.left_column.column_pointer.column->num_rows); i++){
            payload[i] = ((int*)query->operator_fields.binary_operator.left_column.column_pointer.result->payload)[i] -
                    query->operator_fields.binary_operator.right_column.column_pointer.column->data[i];
        }
    } else if (query->operator_fields.binary_operator.left_column.column_type == COLUMN && query->operator_fields.binary_operator.right_column.column_type == RESULT) {
        for (size_t i=0; i < *(query->operator_fields.binary_operator.left_column.column_pointer.column->num_rows); i++){
            payload[i] = query->operator_fields.binary_operator.left_column.column_pointer.column->data[i] -
                    ((int*)query->operator_fields.binary_operator.right_column.column_pointer.result->payload)[i];
        }
    } else {
        for (size_t i=0; i < *(query->operator_fields.binary_operator.left_column.column_pointer.column->num_rows); i++){
            payload[i] = query->operator_fields.binary_operator.left_column.column_pointer.column->data[i] -
                    query->operator_fields.binary_operator.right_column.column_pointer.column->data[i];
        }
    }

    Result* result = malloc(sizeof(Result));
    result->num_tuples = query->operator_fields.binary_operator.left_column.column_pointer.result->num_tuples;
    result->data_type = INT;
    result->payload = payload;
    insert_result_context(result, query->operator_fields.binary_operator.handle, client_context);
    return " ";
}



/**
* append_batch_operator appends a query to a contexts batch_operators array
* the idea is that the execute function will decide how to split up queries
* three cases: 
* new batch and batch_operators is null
* batch_operators array is full and we need to resize
* spare space in batch_operators
*/
char* append_batch_operator(DbOperator* query){
    if (query->context->batch_operators == NULL){
        query->context->batch_operators = malloc(5 * sizeof(DbOperator));
        query->context->batch_operators[0] = *query;
        query->context->batch_operators_in_use = 1;
        query->context->batch_operator_slots = 5;
        return " ";
    }

    if (query->context->batch_operator_slots <= query->context->batch_operators_in_use){
        query->context->batch_operators = realloc(query->context->batch_operators, 2 * query->context->batch_operator_slots * sizeof(DbOperator));
        query->context->batch_operator_slots = 2 * query->context->batch_operator_slots;
    }

    query->context->batch_operators[query->context->batch_operators_in_use] = *query;
    query->context->batch_operators_in_use += 1;

    return " ";
}



/*
* Function passed to execute a shared scan. Meant to be passed to pthread, but could run standalone
* TODO need to write a struct to pass arguments and to store results. 
*/

void* execute_shared_select(void* thread_args){
    SharedScanArgs* args = (SharedScanArgs*) thread_args;
    if (args->data_type != INT){
        log_err("Attempted to do share scan on non int data");
        return NULL;
    }

    // pthread_t thId = pthread_self();

    int* data = (int*) args->data;    

    // printf("num queries %d, vect_size %d \n", args->num_queries, args->vect_size );
    // size_t sel_in_vector = 0;
    const size_t offset = args->position_offset;
    const size_t vect_size = args->vect_size;
    const size_t num_queries = args->num_queries;

    for (size_t i=0; i < vect_size; i++){
        for (size_t k=0; k < num_queries; k++){
            ((size_t*)args->results[k]->payload)[args->results[k]->num_tuples] = offset + i;
            // printf("id: %d : data: %d\n", thId, data[i]);
            args->results[k]->num_tuples += ((data[i] >= args->comps[k].p_low) & (data[i] < args->comps[k].p_high));            
        }
    }   
    return NULL;
}


/*
* This function callss functions defined in db_index.h
* These will either create a unclustered index, or mark that a clustered index will
* need to be built when the data is loaded. 
*/


char* create_index(DbOperator* query){
    if (INDEXES == 0){
        return "";
    }
    if (query->operator_fields.create_operator.index_type == SORTED && query->operator_fields.create_operator.clustered == true){
        if (create_clustered_sorted_index(query->operator_fields.create_operator.column) < 0){
            log_err("Error creating clustered sorted index");
        }

    } else if (query->operator_fields.create_operator.index_type == SORTED && query->operator_fields.create_operator.clustered == false){
        if (create_unclustered_sorted_index(query->operator_fields.create_operator.column) < 0){
            log_err("Error creating unclustered sorted index");
        }
    } else if (query->operator_fields.create_operator.index_type == BTREE && query->operator_fields.create_operator.clustered == true){
        if (create_clustered_btree_index(query->operator_fields.create_operator.column) < 0){
            log_err("Error creating unclustered sorted index");
        }
    } else if (query->operator_fields.create_operator.index_type == BTREE && query->operator_fields.create_operator.clustered == false){
        if (create_unclustered_btree_index(query->operator_fields.create_operator.column) < 0){
            log_err("Error creating unclustered sorted index");
        }
    }

    return "";
}

/*
* execute_batch rearranges the queries into sub batches
* currently spins up threads for each vector iteration
* may want to look into performance of this versus using a thread pool which 
* would be a lot more effort to sync on each vector completion.
*/

char* execute_batch(ClientContext* client_context){
    // num_sub_batches is the total number of sub batches
    // we may not run them all at the same time due to number of available logical threads/tlb size
    size_t num_sub_batches = client_context->batch_operators_in_use / QUERIES_PER_THREAD;
    //ensure we have the right number of threads. 
    if (num_sub_batches == 0 || client_context->batch_operators_in_use % QUERIES_PER_THREAD != 0) {
        num_sub_batches += 1;
    }

    //array of args to be passed to each thread call. 
    SharedScanArgs ss_args[num_sub_batches];
    const size_t num_rows = *(client_context->batch_operators[0].operator_fields.select_operator.src->column_pointer.column->num_rows);

    // j indexes sub batches, i indexes into queries per thread with k offset. 
    size_t k = 0;
    for (size_t j=0; j < num_sub_batches; j ++){
        ss_args[j].num_queries = 0;
        for (size_t i=0; i < QUERIES_PER_THREAD; i++){
            if (k + i >= client_context->batch_operators_in_use){
                break;
            }
            if (client_context->batch_operators[i].type != SELECT){
                // printf("enum operator type %d\n", client_context->batch_operators[i].type);
                return "Batched operators currently only supports selects";
            }
            
            ss_args[j].comps[i].p_low = client_context->batch_operators[k+i].operator_fields.select_operator.minimum;
            ss_args[j].comps[i].p_high = client_context->batch_operators[k+i].operator_fields.select_operator.maximum;
            // printf("low: %d, high %d \n", ss_args[j].comps[i].p_low, ss_args[j].comps[i].p_high );
            ss_args[j].data_type = INT;

            Result* result = malloc(sizeof(Result));
            result->num_tuples = 0;
            result->num_update_tuples = 0;
            result->data_type = POSITIONLIST; 
            result->payload = malloc(sizeof(int) * num_rows);
            // printf("result handles %s\n",  client_context->batch_operators[k+i].operator_fields.select_operator.handle);
            insert_result_context(result, client_context->batch_operators[k+i].operator_fields.select_operator.handle, client_context);
            ss_args[j].results[i] = result;
            ss_args[j].num_queries +=1;
        }
        k += QUERIES_PER_THREAD;
    }  

    size_t num_batches = num_sub_batches / QUERIES_PER_THREAD;
    if (num_batches == 0 || num_batches % QUERIES_PER_THREAD != 0) {
        num_batches += 1;
    }

    size_t num_vectors = num_rows / SS_VECTOR_SIZE;
    if (num_vectors == 0 || num_rows % SS_VECTOR_SIZE != 0) {
            num_vectors += 1;
        }


    pthread_t* threads = malloc(NUM_THREADS * sizeof(pthread_t));

    // still only supporting int data, to support other types we would check the passed datatype. 
    int* data = client_context->batch_operators[0].operator_fields.select_operator.src->column_pointer.column->data;

    // printf(" num sub batches %d\n", num_sub_batches );
    size_t completed_sub_batches = 0;
    for (size_t i=0; i<num_batches; i++){
        size_t v_index = 0;
        for (size_t j=0; j < num_vectors; j++){
            size_t vector_index = j * SS_VECTOR_SIZE;

            int* vector = &data[vector_index];
            // printf("start of vector %d with index %ld \n", vector[0], vector_index);
            // if remaining # rows < vector size then use vector size, else use remaining number of rows. 
            size_t vector_length = num_rows - vector_index < SS_VECTOR_SIZE ? num_rows - vector_index : SS_VECTOR_SIZE;
            // printf(" v index  %d , v length%d , data index %d\n", v_index, vector_length, vector_index);
            // printf(" num rows %d\n", num_rows );
            v_index += vector_length;
            for (size_t k=0; k < NUM_THREADS; k++){
                // printf("i:%d, num completed_sub_batches %d, k %d \n",i, completed_sub_batches, k );
                if (k + completed_sub_batches >= num_sub_batches){
                    break;
                }
                // in future may want to play around with pinning threads to specific CPU cores
                ss_args[completed_sub_batches+k].data = vector;
                ss_args[completed_sub_batches+k].vect_size = vector_length;
                ss_args[completed_sub_batches+k].position_offset = vector_index;
                pthread_create(&threads[k], NULL, &execute_shared_select, (void*) (&ss_args[completed_sub_batches+k]));
            }

            for (size_t k=0; k<NUM_THREADS; k++){
                if (k + completed_sub_batches >= num_sub_batches){
                    break;
                }
                // block on waiting for threads to finish
                if(pthread_join(threads[k], NULL) != 0){
                    log_err("%s:%d Failed to join pthread", __FILE__, __LINE__);
                }
                // printf("Joined thread #%d\n", k);
            }
        }
        completed_sub_batches += NUM_THREADS;
    }
    // printf(" returning from ss select \n");
    return " ";
}



// ****************************************************************************
// Joins
// ****************************************************************************




/*
* first implementation will be single threaded, but this is where I would vectorize 
* Split up partitions across cores a vector at a time so that the vector is in l3
* then merge together in this thread.
*/
void* radix_parition_thread(void* radix_args){
    PartitionThreadArgs* args = (PartitionThreadArgs*) radix_args;

    int bit_mask = args->num_parts - 1;
    for (size_t i=0; i < args->data_length; i++){
        // printf("part %d, pos%ld, val %d \n",args->vals[i] & bit_mask, args->positions[i],  args->vals[i]);
        args->pos_partitions[args->vals[i] & bit_mask][args->partition_lengths[args->vals[i] & bit_mask]] = args->positions[i];
        args->val_partitions[args->vals[i] & bit_mask][args->partition_lengths[args->vals[i] & bit_mask]] = args->vals[i];
        args->partition_lengths[args->vals[i] & bit_mask] += 1;
    }
    return NULL;
}

/*
* Starting with a single pass, paritions both columns at the same time
* next stop would be to partition on two cores
* could multi thread by partitioning a vector at a time and have different cores responsible 
* for different bits, also gets around the TLB limit since each core has its own TLB.
* num_parts MUST be a power of 2
*/
JoinPartitions* radix_parition(size_t num_parts, size_t* left_pos, size_t* right_pos, int* left_val, int* right_val, size_t left_len, size_t right_len){
    // prepare args for left thread and right thread
    PartitionThreadArgs* left_args = calloc(1, sizeof(PartitionThreadArgs));
    PartitionThreadArgs* right_args = calloc(1, sizeof(PartitionThreadArgs));

    left_args->positions = left_pos;
    left_args->vals = left_val;
    left_args->data_length = left_len;
    left_args->num_parts = num_parts;
    left_args->partition_lengths = calloc(num_parts, sizeof(size_t));
    left_args->pos_partitions = malloc(sizeof(size_t*) * num_parts);
    left_args->val_partitions = malloc(sizeof(int*) * num_parts);

    right_args->positions = right_pos;
    right_args->vals = right_val;
    right_args->data_length = right_len;
    right_args->num_parts = num_parts;
    right_args->partition_lengths = calloc(num_parts, sizeof(size_t));
    right_args->pos_partitions = malloc(sizeof(size_t*) * num_parts);
    right_args->val_partitions = malloc(sizeof(int*) * num_parts);


    for (size_t i=0; i < num_parts; i++){
        left_args->pos_partitions[i] = malloc(sizeof(size_t) * left_len);
        left_args->val_partitions[i] = malloc(sizeof(int) * left_len);
        right_args->pos_partitions[i] = malloc(sizeof(size_t) * right_len);
        right_args->val_partitions[i] = malloc(sizeof(int) * right_len);
    }

    pthread_t* threads = malloc(2 * sizeof(pthread_t));

    pthread_create(&threads[0], NULL, &radix_parition_thread, (void*) (left_args));
    pthread_create(&threads[1], NULL, &radix_parition_thread, (void*) (right_args));

    if(pthread_join(threads[0], NULL) != 0){
        log_err("%s:%d Failed to join pthread", __FILE__, __LINE__);
    }

    if(pthread_join(threads[1], NULL) != 0){
        log_err("%s:%d Failed to join pthread", __FILE__, __LINE__);
    }

    JoinPartitions* part_struct = malloc(sizeof(JoinPartitions));
    part_struct->num_parts = num_parts;
    part_struct->left_pos_partitions = left_args->pos_partitions;
    part_struct->left_val_partitions = left_args->val_partitions;
    part_struct->left_partition_lengths = left_args->partition_lengths;
    part_struct->right_pos_partitions = right_args->pos_partitions;
    part_struct->right_val_partitions = right_args->val_partitions;
    part_struct->right_partition_lengths = right_args->partition_lengths;

    return part_struct;
}



// need to malloc space for join positions
void* hash_join_thread(void* hj_args){
    HashJoinThreadArgs* args = (HashJoinThreadArgs*) hj_args;
    HashTable* ht;

    size_t alloc_size = args->left_len > args->right_len ? args->left_len : args->right_len;

    args->left_join_positions = malloc(alloc_size * sizeof(size_t));
    args->right_join_positions = malloc(alloc_size * sizeof(size_t));

    // printf("Hash join left len %ld right len %ld\n", args->left_len, args->right_len);
    // raise(SIGINT);
    // 12 is arbitrary
    // ptr to ptr because I want to minimise memory allocs and the get function will realloc
    // only if it need to, but I also don't want to loose track of the ret_pos pointer value
    size_t* ret_pos = malloc(sizeof(size_t) * 12);
    size_t** ret_pos_ptr = malloc(sizeof(size_t*));
    *ret_pos_ptr = ret_pos;
    size_t value_init_length = 12;
    size_t num_values; // number if items returned by the hash lookup
    // Create hash table on smalled side and iterate through larger side. 
    if (args->left_len > args->right_len){
        ht = ht_allocate(args->right_len);
        bulk_ht_load(ht, args->right_vals, args->right_pos, args->right_len);
        
        for (size_t i = 0; i < args->left_len; i++){
            num_values = 0;
            
            get(ht, args->left_vals[i], ret_pos_ptr, &num_values, &value_init_length);
            // printf("Num values in threads %ld \n", num_values );
            
            if (args->join_len + num_values> alloc_size){
                args->left_join_positions = realloc(args->left_join_positions, 2 * alloc_size * sizeof(size_t));
                args->right_join_positions = realloc(args->right_join_positions, 2 * alloc_size * sizeof(size_t));
                alloc_size = 2 * alloc_size;
            }
            for (size_t j = 0; j < num_values; j ++){
                // printf("Probe val %d, ret pos %ld \n", args->left_vals[i], ret_pos[j] );
                args->left_join_positions[args->join_len] = args->left_pos[i];
                args->right_join_positions[args->join_len] = (*ret_pos_ptr)[j];
                args->join_len += 1;
            }
        }


    } else { 
        // left smaller

        ht = ht_allocate(args->left_len);
        bulk_ht_load(ht, args->left_vals, args->left_pos, args->left_len);
        for (size_t i = 0; i < args->right_len; i++){
            num_values = 0;
            
            get(ht, args->right_vals[i], ret_pos_ptr, &num_values, &value_init_length);
            // printf("Num values %ld\n", num_values );
            
            if (args->join_len + num_values > alloc_size){
                args->left_join_positions = realloc(args->left_join_positions, 2 * alloc_size * sizeof(size_t));
                args->right_join_positions = realloc(args->right_join_positions, 2 * alloc_size * sizeof(size_t));
                alloc_size = 2 * alloc_size;
            }
            for (size_t j = 0; j < num_values; j ++){
                // printf("Probe val %d, ret pos %ld \n", args->left_vals[i], ret_pos[j] );
                args->left_join_positions[args->join_len] = (*ret_pos_ptr)[j];
                args->right_join_positions[args->join_len] = args->right_pos[i];
                args->join_len += 1;
            }

        }
    }

    args->left_join_positions = realloc(args->left_join_positions, args->join_len * sizeof(size_t));
    args->right_join_positions = realloc(args->right_join_positions, args->join_len * sizeof(size_t));
    ht_deallocate(ht);
    return NULL;
}

char* execute_hash_join(DbOperator* query, ClientContext* client_context, int* left_data, size_t* left_pos,
                             int* right_data, size_t* right_pos, size_t left_length, size_t right_length){
    size_t num_parts = TLB / 2;
    JoinPartitions* parts = radix_parition(num_parts, left_pos, right_pos, left_data, right_data, left_length, right_length);

    // for (size_t i =0; i < num_parts; i++){
    //     printf("Left partition\n");
    //     for (size_t j = 0; j < parts->left_partition_lengths[i]; j++){
    //         printf("val %d, pos %ld\n", parts->left_val_partitions[i][j], parts->left_pos_partitions[i][j] );

    //     }
    //     printf("right partition\n");
    //     for (size_t j = 0; j < parts->right_partition_lengths[i]; j++){
    //          printf("val %d, pos %ld\n", parts->right_val_partitions[i][j], parts->right_pos_partitions[i][j] );
    //     }
    // }
    

    // Spining off theads, keeping in mind that partitions need to fit in L3 cache. 
    // icremented when thread is spawned 

    Result* left_res = malloc(sizeof(Result));
    Result* right_res = malloc(sizeof(Result));
    left_res->data_type = POSITIONLIST;
    right_res->data_type = POSITIONLIST;
    left_res->payload = malloc(sizeof(size_t) * left_length);
    right_res->payload = malloc(sizeof(size_t) * left_length);
    size_t payload_alloc_length = left_length;
    left_res->num_tuples = 0;
    right_res->num_tuples = 0;
    left_res->num_update_tuples = 0;
    right_res->num_update_tuples = 0;

    size_t completed_parts = 0;

    pthread_t* threads = malloc(JOINTHREADS * sizeof(pthread_t));

    HashJoinThreadArgs** thread_args_arr = malloc(sizeof(HashJoinThreadArgs*) * JOINTHREADS);

    while (completed_parts < num_parts){
        size_t threads_to_create = (JOINTHREADS > num_parts - completed_parts) ? JOINTHREADS : num_parts - completed_parts;
        size_t threads_in_use = 0;
        // printf("Threads to create %ld\n", threads_to_create);
        for (size_t i=0; i < threads_to_create; i++){
            while (parts->left_partition_lengths[completed_parts] == 0 || parts->right_partition_lengths[completed_parts] == 0){
                completed_parts += 1;
            }

            if (completed_parts > num_parts){
                break;
            }
            // printf("completed_parts %ld \n", completed_parts);
            // printf("left part size %ld right part size %ld \n", parts->left_partition_lengths[completed_parts], parts->right_partition_lengths[completed_parts] );
            thread_args_arr[threads_in_use] = calloc(1, sizeof(HashJoinThreadArgs));
            thread_args_arr[threads_in_use]->left_pos = parts->left_pos_partitions[completed_parts];
            thread_args_arr[threads_in_use]->right_pos = parts->right_pos_partitions[completed_parts];
            thread_args_arr[threads_in_use]->left_vals = parts->left_val_partitions[completed_parts];
            thread_args_arr[threads_in_use]->right_vals = parts->right_val_partitions[completed_parts];
            thread_args_arr[threads_in_use]->left_len = parts->left_partition_lengths[completed_parts];
            thread_args_arr[threads_in_use]->right_len = parts->right_partition_lengths[completed_parts];
            
            // printf(" pre pthread create: Hash join left len %ld right len %ld\n", thread_args_arr[threads_in_use]->left_len, thread_args_arr[threads_in_use]->right_len);
            
            pthread_create(&threads[threads_in_use], NULL, &hash_join_thread, (void*) (thread_args_arr[threads_in_use]));
            completed_parts += 1;
            threads_in_use += 1;
        }

        // Join all threads, copy results to left and right results
        for (size_t i=0; i < threads_in_use; i++){
            if(pthread_join(threads[i], NULL) != 0){
                log_err("%s:%d Failed to join pthread num %ld ", __FILE__, __LINE__, i);
            } 
        }

        for (size_t i=0; i < threads_in_use; i++){
            // resize results as needed
            if (thread_args_arr[i]->join_len + left_res->num_tuples > payload_alloc_length){
                left_res->payload = realloc(left_res->payload, (2*thread_args_arr[i]->join_len + left_res->num_tuples) * sizeof(size_t));
                right_res->payload = realloc(right_res->payload, (2*thread_args_arr[i]->join_len + right_res->num_tuples) * sizeof(size_t));
            }

            memcpy(&(((size_t*) left_res->payload)[left_res->num_tuples]), thread_args_arr[i]->left_join_positions, thread_args_arr[i]->join_len * sizeof(size_t));
            memcpy(&(((size_t*) right_res->payload)[right_res->num_tuples]), thread_args_arr[i]->right_join_positions, thread_args_arr[i]->join_len * sizeof(size_t));
            // raise(SIGINT);
            // printf("i%ld Join len as results copied %ld \n", i, thread_args_arr[i]->join_len);
            left_res->num_tuples += thread_args_arr[i]->join_len;
            right_res->num_tuples += thread_args_arr[i]->join_len;
            free(thread_args_arr[i]);
        }

    }

    // for (size_t i=0; i < left_res->num_tuples; i++){
    //     printf("left res pos %ld, right res_pos %ld \n", ((size_t*)left_res->payload)[i], ((size_t*)right_res->payload)[i] );
    // }

    // printf("left handle name %s,  right_handle name %s \n",query->operator_fields.join_operator.handle_left, query->operator_fields.join_operator.handle_right);
    // printf(" num_matches %ld \n", left_res->num_tuples);
    insert_result_context(left_res, query->operator_fields.join_operator.handle_left, client_context);
    insert_result_context(right_res, query->operator_fields.join_operator.handle_right, client_context);
    return "";
}

/*
* I am pretty sure all arguments will be results as opposed to columns, since positions
* also need to be passed in.
*/

char* execute_nestedloop_join(DbOperator* query, ClientContext* client_context, int* left_data, size_t* left_pos,
                             int* right_data, size_t* right_pos, size_t left_length, size_t right_length){

    // printf("Nested loop join start\n");
    
    
    Result* left_res = malloc(sizeof(Result));
    Result* right_res = malloc(sizeof(Result));
    left_res->data_type = POSITIONLIST;
    right_res->data_type = POSITIONLIST;

    size_t result_alloc_size = left_length > right_length ? left_length : right_length;
    left_res->payload = malloc(sizeof(size_t) * result_alloc_size);
    right_res->payload = malloc(sizeof(size_t) * result_alloc_size);


    /*
    * This can be done on multiple cores, breaking the larger value vector down into smaller vectors.
    * even without multi core vectorizing it will improve cache performance 
    */
    size_t num_matches = 0;
    for (size_t i=0; i < left_length; i++){
        if (result_alloc_size < num_matches + right_length){
            left_res->payload = realloc(left_res->payload, sizeof(size_t) * (result_alloc_size + num_matches));
            right_res->payload = realloc(right_res->payload, sizeof(size_t) * (result_alloc_size + num_matches));
        }
        for (size_t j=0; j < right_length; j++){
            ((size_t*)left_res->payload)[num_matches] = left_pos[i];
            ((size_t*)right_res->payload)[num_matches] = right_pos[j];
            num_matches += left_data[i] == right_data[j];
        }
    }

    left_res->payload = realloc(left_res->payload, sizeof(size_t) * (num_matches));
    right_res->payload = realloc(right_res->payload, sizeof(size_t) * (num_matches));
    left_res->num_tuples = num_matches;
    left_res->num_update_tuples = 0;
    right_res->num_tuples = num_matches;
    right_res->num_update_tuples = 0;
    // printf("left handle name %s,  right_handle name %s \n",query->operator_fields.join_operator.handle_left, query->operator_fields.join_operator.handle_right);
    // printf(" num_matches %ld \n", num_matches);
    insert_result_context(left_res, query->operator_fields.join_operator.handle_left, client_context);
    insert_result_context(right_res, query->operator_fields.join_operator.handle_right, client_context);
    return "";
}


/*
* Consider pros/cons of passing left and right pos and val arrays to the join 
* functions rather than calculating inside of them 
*/

char* execute_join(DbOperator* query, ClientContext* client_context){
    int* left_data;
    size_t* left_pos;
    int* right_data;
    size_t* right_pos;
    size_t left_length;
    size_t right_length;

    if (query->operator_fields.join_operator.left_val->column_pointer.result->data_type != INT){
        return "Left value vector must be integer type\n";
    }
    left_data = (int*) query->operator_fields.join_operator.left_val->column_pointer.result->payload;
    left_length = query->operator_fields.join_operator.left_val->column_pointer.result->num_tuples;

    if (query->operator_fields.join_operator.right_val->column_pointer.result->data_type != INT){
        return "Right value vector must be integer type\n";
    }
    right_data = (int*) query->operator_fields.join_operator.right_val->column_pointer.result->payload;
    right_length = query->operator_fields.join_operator.right_val->column_pointer.result->num_tuples;

    if (query->operator_fields.join_operator.left_pos->column_pointer.result->data_type != POSITIONLIST){
        return "Left position vector must be integer type\n";
    }
    left_pos = (size_t*) query->operator_fields.join_operator.left_pos->column_pointer.result->payload;

    if (query->operator_fields.join_operator.right_pos->column_pointer.result->data_type != POSITIONLIST){
        return "Right position vector must be integer type\n";
    }
    right_pos = (size_t*) query->operator_fields.join_operator.right_pos->column_pointer.result->payload;



    if (query->operator_fields.join_operator.join_type == HASH){
        return execute_hash_join(query, client_context, left_data, left_pos, right_data, right_pos, left_length, right_length);
    } else if (query->operator_fields.join_operator.join_type == NESTEDLOOP){
        // printf("Nested loop about to be called \n");
        return execute_nestedloop_join(query, client_context, left_data, left_pos, right_data, right_pos, left_length, right_length);
    } else {
        return "Only Hash and loop joins are implemented";
    }
}


// ****************************************************************************
// Insert, Delete, Update
// ****************************************************************************



char* execute_insert(DbOperator* query) {
    // old non differential insert 
    // size_t end_of_data = query->operator_fields.insert_operator.table->table_length;
    // if (end_of_data == query->operator_fields.insert_operator.table->table_alloc_size){
    //     resize_table(query->operator_fields.insert_operator.table,
    //         2 * query->operator_fields.insert_operator.table->table_alloc_size);
    //     ;
    // }
    // for (size_t i=0; i < query->operator_fields.insert_operator.table->col_count; i++){
    //     query->operator_fields.insert_operator.table->columns[i].data[end_of_data] = query->operator_fields.insert_operator.values[i];
    // }
    // query->operator_fields.insert_operator.table->table_length += 1;
    // return "";

    Table* table = query->operator_fields.insert_operator.table;

    if (table->columns[0].update_struct.ins_length >= UPDATE_BATCH_SIZE){
        flush_updates(table);
    }

    for (size_t i=0; i < table->col_count; i++){
        printf("inse length in update %ld \n", table->columns[i].update_struct.ins_length);
        table->columns[i].update_struct.ins_val[table->columns[i].update_struct.ins_length] = query->operator_fields.insert_operator.values[i];
        table->columns[i].update_struct.ins_length += 1;
        // raise(SIGINT);

    }

    return "";
}


char* execute_delete(DbOperator* query) {
    printf("db delete\n");
    Table* table = query->operator_fields.delete_operator.table;
    Result* positions = query->operator_fields.delete_operator.positions;
    // old non differential delets
    // delete from base data first
    // size_t init_num_rows = table->table_length;
    // for (size_t i=0; i < table->col_count; i++){
    //     size_t local_num_rows = init_num_rows;
    //     for (size_t j = 0; j < positions->num_tuples; j++){
    //         memmove(&table->columns[i].data[((size_t*)positions->payload)[j]], 
    //             &table->columns[i].data[((size_t*)positions->payload)[j]+1],
    //             (local_num_rows - ((size_t*)positions->payload)[j]) * sizeof(int));
    //         local_num_rows -= 1;    
    //     }
    
    // table->table_length -= positions->num_tuples;
    // }

    
     for (size_t j = 0; j < positions->num_tuples; j++){
        if (table->columns[0].update_struct.del_length >= UPDATE_BATCH_SIZE){
            
            flush_updates(table);
        }


        for (size_t i=0; i < table->col_count; i++){
            //remove from pending inserts if it is there, else append to pending deletes
            
            if (((size_t *)positions->payload)[j] > table->table_length){
                size_t ins_offset = ((size_t *)positions->payload)[j] - table->table_length;
                memmove(&table->columns[i].update_struct.ins_val[ins_offset], 
                    &table->columns[i].update_struct.ins_val[ins_offset+1], 
                    (table->columns[i].update_struct.ins_length - ins_offset) * sizeof(size_t));
                table->columns[i].update_struct.ins_length -= 1;
            } else{
                table->columns[i].update_struct.del_pos[table->columns[i].update_struct.del_length] = ((size_t *)positions->payload)[j];
                table->columns[i].update_struct.del_length += 1;
            }
            
        }
    }

    return "";
}

char* execute_update(DbOperator* query) {
    Column* column = query->operator_fields.update_operator.column;
    Result* positions = query->operator_fields.update_operator.positions;
    printf("running update \n");
    for (size_t i=0; i < positions->num_tuples - positions->num_update_tuples; i++){
        printf("prev value %d, new value %d \n",column->data[((size_t*)positions->payload)[i]], query->operator_fields.update_operator.value);
        column->data[((size_t*)positions->payload)[i]] = query->operator_fields.update_operator.value;
    }

    for (size_t i=0; i < positions->num_update_tuples; i++){
        printf("prev value %d, new value %d \n", column->update_struct.ins_val[((size_t*)positions->payload)[i] - *column->num_rows], query->operator_fields.update_operator.value);
        column->update_struct.ins_val[((size_t*)positions->payload)[i] - *column->num_rows] = query->operator_fields.update_operator.value;
    }

    return "";
}



/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/
char* execute_DbOperator(DbOperator* query, ClientContext* client_context) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    if(!query){   
        log_err("Query is null\n");
        return "Error processing query";
    }

    char* res_string = NULL;

    // Separate code branches for batching and non-batching

    if (query->context->batching_active == true){
        // printf("batched switch statement \n");
        switch(query->type){
            case BATCH_QUERIES:
                log_err("Attempt to start new batch while already batching");
                res_string = "Cannot start a new batch inside an exisiting batch\n";
                break;
            case SELECT:
                res_string = append_batch_operator(query);
                break;
            case BATCH_EXECUTE:
                res_string = execute_batch(client_context);
                query->context->batching_active = false;
                break;
            default:
                log_err("currently only batched selects are implemented");
        }
    } else {
        switch(query->type) {
            case CREATE:
                if(query->operator_fields.create_operator.create_type == _DB){
                    if (create_db(query->operator_fields.create_operator.name).code == OK) {
                        free(query);
                        return " ";
                        // return "Succeeded: created new database";
                    } else {
                        log_err("adding a database failed. \n");
                        free(query);
                        return " ";
                        // return "Failed";
                    }
                } else if (query->operator_fields.create_operator.create_type == _TABLE){
                    Status create_status;
                    create_table(query->operator_fields.create_operator.db, 
                        query->operator_fields.create_operator.name, 
                        query->operator_fields.create_operator.col_count, 
                        &create_status);
                    if (create_status.code != OK) {
                        log_err("adding a table failed. \n");
                        free(query);
                        return " ";
                        // return "Failed";
                    }
                    free(query);
                    // return "Succeeded: created new table";
                    return " ";
                } else if (query->operator_fields.create_operator.create_type == _COLUMN){
                    Status create_status;
                    create_column(query->operator_fields.create_operator.table,
                                  query->operator_fields.create_operator.name,
                                  false, &create_status);
                    free(query);
                    // return "Added column";
                    return " ";
                    if (create_status.code != OK) {
                        log_err("adding a table failed. \n");
                        free(query);
                        return " ";
                        // return "Failed";
                    }
                } else if (query->operator_fields.create_operator.create_type == _INDEX){
                    res_string = create_index(query);
                }
                break;
            case PRINT:
                res_string = execute_print(query);
                break;
            case SELECT:
                res_string = execute_select(query, client_context);
                break;
            case INSERT:
                res_string = execute_insert(query);
                break;
            case DELETE:
                res_string = execute_delete(query);
                break;
            case UPDATE:
                res_string = execute_update(query);
                break;
            case SHUTDOWN:
                shutdown_server();
                // res_string = "Shutting down database";
                res_string = " ";
                break;
            case FETCH:
                res_string = execute_fetch(query, client_context);
                break;
            case LOAD:
                res_string = execute_load(query);
                break;
            case AVG:
                res_string = execute_avg(query, client_context);
                break;
            case SUM:
                res_string = execute_sum(query, client_context);
                break;
            case MIN:
                res_string = execute_min(query, client_context);
                break;
            case MAX:
                res_string = execute_max(query, client_context);
                break;
            case ADD:
                res_string = execute_add(query, client_context);
                break;
            case SUB:
                res_string = execute_sub(query, client_context);
                break;
            case BATCH_QUERIES:
                query->context->batching_active = true;
                res_string = " ";
                break;
            case BATCH_EXECUTE:
                query->context->batching_active = false;
                break;
            case PRINT_INDEX:
                res_string = execute_print_index(query);
                break;
            case JOIN:
                res_string = execute_join(query, client_context);
                break;
            default:
                log_err("No matching switch statement for query \n");
        }
    }

    if (res_string == NULL){
        log_err("Res string null \n");
        return "Error with executing query";
    }
    if (query != NULL){
        free(query);
    }
    return res_string;
}



Status shutdown_server(){
    Status shutdown_status;
    if(write_db()){
        shutdown_status.code = OK;
    } else{
        shutdown_status.code = ERROR;
    }
    return shutdown_status;
}