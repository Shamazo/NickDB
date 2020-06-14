/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include <stdbool.h>
#include <assert.h> 
#include "main_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"


/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/


DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    // check that the database argument is the current active database
    if (!g_db || strcmp(g_db->name, db_name) != 0) {
        log_info( "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = g_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}


DbOperator* parse_create_col(char* create_arguments, message* send_message){
    if(create_arguments == NULL){
        return NULL;
    }

    char* col_name = next_token(&create_arguments, &(send_message->status));
    col_name = trim_quotes(col_name);
    char* table_full_name = next_token(&create_arguments, &(send_message->status));

    // make sure we got right number of arguments
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }

    // remove last parenthesis
    int last_char = strlen(table_full_name) - 1;
    if (table_full_name[last_char] != ')') {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

    table_full_name[last_char] = '\0';

    // get database name from db.tb string
    char* table_name = table_full_name;
    while (*table_name != '\0') {
        if (*table_name == '.') {
            *table_name++ = '\0';
            break;
        }
        table_name++;
    }

    if (g_db == NULL){
        log_info( "Cannot create column when there is no active database\n");
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }

    // create DbOperator and return
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.db = g_db;
    dbo->operator_fields.create_operator.table = lookup_table(table_name);
    if (dbo->operator_fields.create_operator.table == NULL){
        log_err("Table not found \n");
        return NULL;
    }
   return dbo;
}


DbOperator* parse_create_index(char* create_arguments){
    create_arguments = trim_parenthesis(create_arguments);
    char* col_name = strsep(&create_arguments, ",");
    char*  table_name= strsep(&col_name, ".");
    table_name= strsep(&col_name, ".");
    col_name = trim_quotes(col_name);

    char* idx_type = strsep(&create_arguments, ",");

    char* cluster_type = strsep(&create_arguments, ",");
    if (cluster_type == NULL || idx_type == NULL || col_name == NULL){
        log_err("%s:%d Bad arguments for create index %s %s %s\n", __FILE__, __LINE__, cluster_type, idx_type, col_name);
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _INDEX;

    if (strncmp(cluster_type, "clustered", 8) == 0){
        dbo->operator_fields.create_operator.clustered = true;
    } else if (strncmp(cluster_type, "unclustered", 10) == 0){
        dbo->operator_fields.create_operator.clustered = false; 
    } else {
        log_err("%s:%d Invalid index type\n", __FILE__, __LINE__, col_name);
        free(dbo);
        return NULL;
    }

    if (strncmp(idx_type, "sorted", 6) ==0){
        dbo->operator_fields.create_operator.index_type = SORTED;     
    } else if (strncmp(idx_type, "btree", 5) == 0){
        dbo->operator_fields.create_operator.index_type = BTREE; 
    } else {
        log_err("%s:%d Invalid index type\n", __FILE__, __LINE__, col_name);
        free(dbo);
        return NULL;
    }
    dbo->operator_fields.create_operator.table = lookup_table(table_name);
    dbo->operator_fields.create_operator.column = lookup_column(dbo->operator_fields.create_operator.table, col_name);
    

    if (dbo->operator_fields.create_operator.table == NULL || dbo->operator_fields.create_operator.column == NULL){
        free(dbo);
        log_err("%s:%d Column or table are null in parse create index \n", __FILE__, __LINE__);
        return NULL;
    }
    return dbo; 
}

DbOperator* parse_print_index(char* query_command){
    // dont need db name which is before first period.
    query_command = trim_parenthesis(query_command);
    split_table_column(&query_command);
    char* table_name = split_table_column(&query_command);
    Table* table= lookup_table(table_name);
    Column* column = lookup_column(table, query_command);

    if (column == NULL){
        printf("%s\n", query_command );
        log_err("%s:%d Column is null in parse create print index \n", __FILE__, __LINE__);
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT_INDEX;
    dbo->operator_fields.print_operator.column = column;

    return dbo;

}


/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments, message* send_message) {
    message_status mes_status = OK_WAIT_FOR_RESPONSE;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            log_info( "INCORRECT_FORMAT\n");
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy, send_message);
            } else if (strcmp(token, "idx") == 0){
                dbo = parse_create_index(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    send_message->status = mes_status;
    if (dbo == NULL || mes_status == INCORRECT_FORMAT){
        log_err("%s:%d DBO is null after parse create \n", __FILE__, __LINE__);
    }
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        //at this point is db.tbl
        char* db_table_name = next_token(command_index, &send_message->status);
        //after this its tbl
        log_err("db table name %s\n", db_table_name);
        split_table_column(&db_table_name);
        char* table_name = split_table_column(&db_table_name);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            log_err("%s:%d Table not found in parse insert\n", __FILE__, __LINE__);
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_delete(char* query_command, ClientContext* context) {
    query_command = trim_parenthesis(query_command);
    char* tbl_name = strsep(&query_command, ",");
    strsep(&tbl_name, ".");
    if (query_command == NULL || tbl_name == NULL){
        log_err("%s:%d table name or pos_vec name null \n", __FILE__, __LINE__);
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));

    Table* table = lookup_table(tbl_name);
    GeneralizedColumn* gen_col = lookup_src(query_command, context);
    Result* pos_vec = gen_col->column_pointer.result;

    if (pos_vec == NULL || table == NULL){
        log_err("%s:%d table or pos_vec  null \n", __FILE__, __LINE__);
        return NULL;
    }

    dbo->type = DELETE;
    dbo->operator_fields.delete_operator.table = table;
    dbo->operator_fields.delete_operator.positions = pos_vec;

    return dbo;
}

DbOperator* parse_update(char* query_command, ClientContext* context) {
    query_command = trim_parenthesis(query_command);
    char* col_name = strsep(&query_command, ",");
    char* vec_pos_name = strsep(&query_command, ",");

    if (col_name == NULL || vec_pos_name == NULL || query_command == NULL){
        log_err("%s:%d col name or pos_vec name or value null \n", __FILE__, __LINE__);
        return NULL;
    }

    GeneralizedColumn* gen_col = lookup_src(col_name, context);
    Column* col = gen_col->column_pointer.column;
    GeneralizedColumn* pos_gencol_vec = lookup_src(vec_pos_name, context);
    Result* pos_vec = pos_gencol_vec->column_pointer.result;
    int value = atoi(query_command);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = UPDATE;
    dbo->operator_fields.update_operator.column = col;
    dbo->operator_fields.update_operator.positions = pos_vec;
    dbo->operator_fields.update_operator.value = value;

    return dbo;
}



DbOperator* parse_shutdown(){
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SHUTDOWN;
    return dbo;
}


DbOperator* parse_select(char* query_command, message* send_message, char* handle, ClientContext* context){
    if (send_message == NULL)
        return NULL;
    if (send_message == NULL || *query_command != '(') {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    query_command++;

    // create a copy of string
    size_t space = strlen(query_command) + 1;
    char* copy = malloc(space * sizeof(char));
    char* to_free = copy;
    strcpy(copy, query_command);
    size_t len = strlen(copy);
    if (copy[len - 1] != ')') {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    copy[len - 1] = '\0';
    
    // parse arguments
    char* arg1 = next_token(&copy, &send_message->status);
    char* arg2 = next_token(&copy, &send_message->status);
    char* arg3 = next_token(&copy, &send_message->status);

    if (arg1 == NULL || arg2 == NULL || arg3 == NULL || send_message->status == INCORRECT_FORMAT) {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    char* arg4 = copy;

    DbOperator* dbo = malloc(sizeof(DbOperator));

    // need to look up source to see if it is in client context, or a column in a table. 
    // need to look up indices in client context only 


    dbo->type = SELECT;
    if (arg4 == NULL){
        //Look up the src column/handle. Look for column first
        
        GeneralizedColumn* src = lookup_src(arg1, context);
        if (src == NULL){
            log_info( "Could not find column or handle: %s\n", arg1);
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        dbo->operator_fields.select_operator = (SelectOperator) {
            .indices = NULL,
            .src = src,
            .minimum = (strcmp("null", arg2) == 0) ? INT_MIN : atoi(arg2),
            .maximum = (strcmp("null", arg3) == 0) ? INT_MAX : atoi(arg3),
            .use_index_vector = false
        };
        strcpy(dbo->operator_fields.select_operator.handle, handle);
    } else {
        log_err("Using index in parse select \n");
        GeneralizedColumn* src = lookup_src(arg2, context);
        if (src == NULL){
            log_info( "Could not find column or handle: %s\n", arg2);
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        GeneralizedColumnHandle* indices = lookup_context(arg1, context);
        if (indices == NULL){
            log_info( "Could not find indices handle handle: %s\n", arg1);
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        dbo->operator_fields.select_operator = (SelectOperator) {
            .indices = indices->generalized_column.column_pointer.result,
            .src = src,
            .minimum = (strcmp("null", arg3) == 0) ? INT_MIN : atoi(arg3),
            .maximum = (strcmp("null", arg4) == 0) ? INT_MAX : atoi(arg4),
            .use_index_vector = true
        };
        strcpy(dbo->operator_fields.select_operator.handle, handle);
    }
    free(to_free);
    return dbo;
}


DbOperator* parse_fetch(char* query_command, message* send_message, char* handle, ClientContext* context){
    if (send_message == NULL)
        return NULL;
    if (query_command == NULL || *query_command != '(') {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    query_command++;

    // create a copy of string
    size_t space = strlen(query_command) + 1;
    char* copy = malloc(space * sizeof(char));
    char* to_free = copy;
    strcpy(copy, query_command);
    size_t len = strlen(copy);
    if (copy[len - 1] != ')') {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    copy[len - 1] = '\0';
    
    // parse arguments
    char* arg1 = next_token(&copy, &send_message->status);
    char* arg2 = copy;

    // dont need db name which is before first period.
    split_table_column(&arg1);
    char* table_name = split_table_column(&arg1);
    Table* table= lookup_table(table_name);
    Column* column = lookup_column(table, arg1);
    if (column == NULL){
        send_message->status = OBJECT_NOT_FOUND;
        log_err("%s:%d Column not found\n", __FILE__, __LINE__);
    }
    

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->operator_fields.fetch_operator.handle = handle;
    dbo->operator_fields.fetch_operator.column = column;
    GeneralizedColumnHandle* index_handle = lookup_context(arg2, context);
    if (index_handle == NULL){
        send_message->status = OBJECT_NOT_FOUND;
        log_err("%s:%d Could not find handle %s \n", arg2, __FILE__, __LINE__);
        return NULL;
    }
    dbo->operator_fields.fetch_operator.indices = index_handle->generalized_column.column_pointer.result; // HOW do I interpret this as a column pointer?
    free(to_free);
    return dbo;
}


DbOperator* parse_print(char* query_command, message* send_message, ClientContext* context){
    if (send_message == NULL)
        return NULL;
    if (query_command == NULL || *query_command != '(') {
        send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    query_command++;

    size_t space = strlen(query_command) + 1;
    char* copy = malloc(space * sizeof(char));
    strcpy(copy, query_command);
    copy[space - 1] = '\0';
    int num_args = 0;
    // see how many args we have before allocating array for them
    while (strsep(&copy, ",") != NULL){
        num_args += 1;
    }
    free(copy);
    copy = malloc(space * sizeof(char));
    char* to_free = copy;
    strcpy(copy, query_command);
    copy[space - 1] = '\0';
    GeneralizedColumn** col_array = malloc(sizeof(GeneralizedColumn*) * num_args);
    int counter = 0;
    size_t col_length;
    while (true) {
        char* arg = strsep(&copy, ",");
        if (arg == NULL){
            break;
        }
        char* copy_2 = malloc(strlen(arg)*sizeof(char) + 1);
        char *to_be_freed = copy_2;
        strcpy(copy_2, arg);
        
        split_table_column(&copy_2);
        char* tbl_name = split_table_column(&copy_2);
        arg = trim_parenthesis(arg);
        GeneralizedColumn* src = lookup_src(arg, context);
        if (src == NULL){
            log_err("Could not find source: %s in parse print \n", arg);
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        col_array[counter] = src;
       
        if (src->column_type == RESULT){
            col_length = src->column_pointer.result->num_tuples;
        } else {
            Table* tbl = lookup_table(tbl_name);
            if (tbl == NULL){
                log_err("%s:%d Could not find table %s", __FILE__, __LINE__, tbl_name);
                return NULL;
            }
            col_length = tbl->table_length;
        }
        free(to_be_freed);
        counter += 1;   
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    dbo->operator_fields.print_operator = (PrintOperator) {
        .columns = col_array,
        .col_count = num_args,
        .col_length = col_length
    };
    free(copy);
    free(to_free);
    return dbo; 

}



DbOperator* parse_load_depreciated(char* query_command){
    // printf("QUERY COMMAND %s\n", query_command );
    if (query_command == NULL || *query_command != '(') {
        return NULL;
    }
    char *copy, *to_free;
    copy = malloc(sizeof(char) * (strlen(query_command)+1));
    to_free = copy;
    strcpy(copy, query_command);
    char* args = strsep(&copy, "\n");
    args = trim_parenthesis(args);
    //here is db.table
    char* table_name = strsep(&args, ",");
    //after this just table
    strsep(&table_name, ".");

    char* str_num_cols = strsep(&args, ",");
    if (str_num_cols == NULL){
        return NULL;
    }
    int num_cols = atoi(str_num_cols);

    char* str_num_rows = strsep(&args, ",");
    if (str_num_rows == NULL){
        return NULL;
    }

    int num_rows = atoi(str_num_rows);
    Table* table = lookup_table(table_name);

    int** values= malloc(sizeof(int*) * num_cols);
    for (int i=0; i < num_cols; i++){
        values[i] = malloc(sizeof(int)*num_rows);
    }

    // char* col_buf = malloc(sizeof(char) * num_rows * 12);
    for (int i=0; i < num_cols; i++){
        char* str_col = strsep(&copy, "\n");
        // printf("%s\n",  str_col);
        for (int j=0; j < num_rows; j++){
            char* str_val = strsep(&str_col, ",");
            if (str_val == NULL){
                // printf("i:%d, j:%d", i, j);
                ;
            } else {
                int int_val = atoi(str_val);
                (values[i])[j] = int_val;
            }
            
        }

    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    dbo->operator_fields.load_operator.table = table;
    dbo->operator_fields.load_operator.num_cols = num_cols;
    dbo->operator_fields.load_operator.num_rows = num_rows;
    dbo->operator_fields.load_operator.values = values;

    // printf("query  for parse load are %s\n", query_command);
    free(to_free);
    return dbo;
}



DbOperator* parse_load(char* query_command){
//     printf("QUERY COMMAND %s\n", query_command );
    printf("parsing load \n");
    if (query_command == NULL) {
        return NULL;
    }

    // get column length / num lines
    size_t num_rows = 0;
    size_t i = 0;
    while (query_command[i] != '\0'){
        num_rows += query_command[i] == '\n';
        i += 1;
    }
    i = 0;
    size_t num_cols = 1;
    while (query_command[i] != '\n'){
        num_cols += query_command[i] == ',';
        i += 1;
    }
    
    // subtract header row
    num_rows -=1;
    // read first row 
    char* first_col_name = calloc(1024, sizeof(char));
    i = 0;
    while (true){
        if (query_command[i] == '\n'){
            break;
        }
        first_col_name[i] = query_command[i];
        i++;
    }
    char* to_free = first_col_name;
    first_col_name = strsep(&first_col_name, ",");
    // drop db name since we dont need it 
    strsep(&first_col_name, ".");
    char* tbl_name = strsep(&first_col_name, ".");

    int** values= malloc(sizeof(int*) * num_cols);
    for (size_t i=0; i < num_cols; i++){
        values[i] = malloc(sizeof(int) * num_rows);
    }

    //skip over head row
    strsep(&query_command, "\n");
    for (size_t i=0; i < num_rows; i++){
        size_t j = 0;
        char* row = strsep(&query_command, "\n");
        while (true) {
            char* val = strsep(&row, ",");
            if (val == NULL || *val == '\n'){
                break;
            }
            int int_val = atoi(val);
            // printf("val: %d\n", int_val);
            (values[j])[i] = int_val;
            j += 1;
        }
    }

    Table* table = lookup_table(tbl_name);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    dbo->operator_fields.load_operator.table = table;
    dbo->operator_fields.load_operator.num_cols = num_cols;
    dbo->operator_fields.load_operator.num_rows = num_rows;
    dbo->operator_fields.load_operator.values = values;

     printf("query  for parse load are %s\n", query_command);
    free(to_free);
    return dbo;


}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/


DbOperator* parse_avg(char* query_command, ClientContext* context, char* handle){
    char* column_handle = trim_parenthesis(query_command);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = AVG;
    GeneralizedColumnHandle* src = lookup_context(column_handle, context);
    if (src == NULL){
        //drop db name
        strsep(&query_command, ".");
        char* table_name = strsep(&query_command, ".");
        if (table_name == NULL){
            log_err("%s:%dNo associated handle and not correct format for column \n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Table* table = lookup_table(table_name);
        if (table == NULL){
            log_err("%s:%d No associated handle in parse_avg and could not find table\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Column* src_column = lookup_column(table, query_command);
        if (src == NULL){
            log_err("%s:%d No associated handle in parse_avg and could not find column\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        dbo->operator_fields.agg_operator.column.column_type = COLUMN;
        dbo->operator_fields.agg_operator.column.column_pointer.column = src_column;
    } else {
        dbo->operator_fields.agg_operator.column = src->generalized_column;
    }

    strcpy(dbo->operator_fields.agg_operator.handle, handle);
    return dbo;
}


DbOperator* parse_min(char* query_command, ClientContext* context, char* handle){
    char* column_handle = trim_parenthesis(query_command);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = MIN;
    GeneralizedColumnHandle* src = lookup_context(column_handle, context);
    if (src == NULL){
        //drop db name
        strsep(&query_command, ".");
        char* table_name = strsep(&query_command, ".");
        if (table_name == NULL){
            log_err("%s:%dNo associated handle and not correct format for column \n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Table* table = lookup_table(table_name);
        if (table == NULL){
            log_err("%s:%d No associated handle in parse_min and could not find table\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Column* src_column = lookup_column(table, query_command);
        if (src == NULL){
            log_err("%s:%d No associated handle in parse_min and could not find column\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        dbo->operator_fields.agg_operator.column.column_type = COLUMN;
        dbo->operator_fields.agg_operator.column.column_pointer.column = src_column;
    } else {
        dbo->operator_fields.agg_operator.column = src->generalized_column;
    }

    strcpy(dbo->operator_fields.agg_operator.handle, handle);
    return dbo;
}


DbOperator* parse_max(char* query_command, ClientContext* context, char* handle){
    char* column_handle = trim_parenthesis(query_command);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = MAX;
    GeneralizedColumnHandle* src = lookup_context(column_handle, context);
    if (src == NULL){
        //drop db name
        strsep(&query_command, ".");
        char* table_name = strsep(&query_command, ".");
        if (table_name == NULL){
            log_err("%s:%dNo associated handle and not correct format for column \n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Table* table = lookup_table(table_name);
        if (table == NULL){
            log_err("%s:%d No associated handle in parse_max and could not find table\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Column* src_column = lookup_column(table, query_command);
        if (src == NULL){
            log_err("%s:%d No associated handle in parse_max and could not find column\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        dbo->operator_fields.agg_operator.column.column_type = COLUMN;
        dbo->operator_fields.agg_operator.column.column_pointer.column = src_column;
    } else {
        dbo->operator_fields.agg_operator.column = src->generalized_column;
    }

    strcpy(dbo->operator_fields.agg_operator.handle, handle);
    return dbo;
}


DbOperator* parse_sum(char* query_command, ClientContext* context, char* handle){
    char* column_handle = trim_parenthesis(query_command);
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SUM;
    GeneralizedColumnHandle* src = lookup_context(column_handle, context);
    if (src == NULL){
        //drop db name
        strsep(&query_command, ".");
        char* table_name = strsep(&query_command, ".");
        if (table_name == NULL){
            log_err("%s:%dNo associated handle and not correct format for column \n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Table* table = lookup_table(table_name);
        if (table == NULL){
            log_err("%s:%d No associated handle in parse_sum and could not find table\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Column* src_column = lookup_column(table, query_command);
        if (src_column == NULL){
            log_err("%s:%d No associated handle in parse_sum and could not find column %s in table %s \n",  __FILE__, __LINE__, query_command, table_name);
            free(dbo);
            return NULL;
        }
        dbo->operator_fields.agg_operator.column.column_type = COLUMN;
        dbo->operator_fields.agg_operator.column.column_pointer.column = src_column;
    } else {
        dbo->operator_fields.agg_operator.column = src->generalized_column;
    }

    strcpy(dbo->operator_fields.agg_operator.handle, handle);
    return dbo;
}


DbOperator* parse_binary_op(char* query_command, ClientContext* context, char* handle){
    char* right_name = trim_parenthesis(query_command);
    char* left_name = strsep(&right_name, ",");
    if (left_name == NULL || right_name == NULL){
        log_err("%s:%d Left or right are NULL in parse add \n",  __FILE__, __LINE__);
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    strcpy(dbo->operator_fields.binary_operator.handle, handle);

    // handle left col first then right 
    GeneralizedColumnHandle* left_col_han = lookup_context(left_name, context);
    if (left_col_han == NULL){
        //drop db name
        strsep(&left_name, ".");
        char* table_name = strsep(&left_name, ".");
        if (table_name == NULL){
            log_err("%s:%d No associated handle and not correct format for column \n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Table* table = lookup_table(table_name);
        if (table == NULL){
            log_err("%s:%d No associated handle in parse_sum and could not find table\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Column* left_column = lookup_column(table, left_name);
        if (left_column == NULL){
            log_err("%s:%d No associated handle in parse_sum and could not find column\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        dbo->operator_fields.binary_operator.left_column.column_type = COLUMN;
        dbo->operator_fields.binary_operator.left_column.column_pointer.column = left_column;
    } else {
        dbo->operator_fields.binary_operator.left_column = left_col_han->generalized_column;
    }


    GeneralizedColumnHandle* right_col_han = lookup_context(right_name, context);
    if (right_col_han == NULL){
        //drop db name
        strsep(&right_name, ".");
        char* table_name = strsep(&right_name, ".");
        if (table_name == NULL){
            log_err("%s:%d No associated handle and not correct format for column \n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Table* table = lookup_table(table_name);
        if (table == NULL){
            log_err("%s:%d No associated handle in parse_sum and could not find table\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        Column* right_column = lookup_column(table, right_name);
        if (right_column == NULL){
            log_err("%s:%d No associated handle in parse_sum and could not find column\n",  __FILE__, __LINE__);
            free(dbo);
            return NULL;
        }
        dbo->operator_fields.binary_operator.right_column.column_type = COLUMN;
        dbo->operator_fields.binary_operator.right_column.column_pointer.column = right_column;
    } else {
        dbo->operator_fields.binary_operator.right_column = right_col_han->generalized_column;
    }
    return dbo;
}


DbOperator* parse_join(char* query_command, ClientContext* context, char* handle){
    query_command = trim_parenthesis(query_command);
    char* left_handle = strsep(&handle, ",");
    if (left_handle == NULL || handle == NULL){
        log_err("%s:%d left or right handle in parse join is NULL\n",  __FILE__, __LINE__);
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = JOIN;
    strcpy(dbo->operator_fields.join_operator.handle_left, left_handle);
    strcpy(dbo->operator_fields.join_operator.handle_right, handle);


    char* val1_name = strsep(&query_command, ",");
    char* pos1_name = strsep(&query_command, ",");

    char* val2_name = strsep(&query_command, ",");
    char* pos2_name = strsep(&query_command, ",");

    printf("%s %s %s %s %s \n",val1_name, pos1_name, val2_name, pos2_name, query_command );
    if (val1_name == NULL || pos1_name == NULL || val2_name == NULL || pos2_name == NULL){
        log_err("%s:%d val or pos names NULL in parse_join \n",  __FILE__, __LINE__);
        free(dbo);
        return NULL;
    }

    dbo->operator_fields.join_operator.left_val = lookup_src(val1_name, context);
    dbo->operator_fields.join_operator.left_pos = lookup_src(pos1_name, context);
    dbo->operator_fields.join_operator.right_val = lookup_src(val2_name, context);
    dbo->operator_fields.join_operator.right_pos = lookup_src(pos2_name, context);

    if (strncmp(query_command, "nested-loop", 11) == 0){
        dbo->operator_fields.join_operator.join_type = NESTEDLOOP;
    } else if (strncmp(query_command, "hash", 4) == 0){
        dbo->operator_fields.join_operator.join_type = HASH;
    } else {
        log_err("%s:%d Invalid join type %s \n",  __FILE__, __LINE__, query_command);
        free(dbo);
        return NULL;
    }

    return dbo;
}


DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }
    // flag to indicate that next incoming message is a file
    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    // log_info( "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    //white space is used in load command
    if (strncmp(query_command, "load", 4) == 0){
        context->incoming_load = true;
    } else if (context->incoming_load == true){
        dbo = parse_load(query_command);
        context->incoming_load = false;
    } else {
        query_command = trim_whitespace(query_command);
        // check what command is given. 
        if (strncmp(query_command, "create", 6) == 0) {
            query_command += 6;
            // log_info( "%s \n", query_command);
            dbo = parse_create(query_command, send_message);
        } else if (strncmp(query_command, "relational_insert", 17) == 0) {
            query_command += 17;
            dbo = parse_insert(query_command, send_message);
        } else if (strncmp(query_command, "relational_delete", 17) == 0) {
            query_command += 17;
            dbo = parse_delete(query_command, context);
        } else if (strncmp(query_command, "relational_update", 17) == 0) {
            query_command += 17;
            dbo = parse_update(query_command, context);
        } else if (strncmp(query_command, "shutdown", 8) ==0){
            query_command += 8;
            dbo = parse_shutdown();
        } else if (strncmp(query_command, "select", 6) == 0){
            query_command += 6;
            dbo = parse_select(query_command, send_message, handle, context);
        } else if (strncmp(query_command, "fetch", 5) == 0){
            query_command += 5;
            dbo = parse_fetch(query_command, send_message, handle, context);
        } else if (strncmp(query_command, "print_index", 11) == 0){
            query_command += 11;
            dbo = parse_print_index(query_command);
        } else if (strncmp(query_command, "print", 5) == 0){
            query_command += 5;
            dbo = parse_print(query_command, send_message, context);
        } else if (strncmp(query_command, "avg", 3) == 0){
            query_command += 3;
            dbo = parse_avg(query_command, context, handle);
        } else if (strncmp(query_command, "min", 3) == 0){
            query_command += 3;
            dbo = parse_min(query_command, context, handle);
        } else if (strncmp(query_command, "max", 3) == 0){
            query_command += 3;
            dbo = parse_max(query_command, context, handle);
        } else if (strncmp(query_command, "sum", 3) == 0){
            query_command += 3;
            dbo = parse_sum(query_command, context, handle);
        } else if (strncmp(query_command, "add", 3) == 0){
            query_command += 3;
            dbo = parse_binary_op(query_command, context, handle);
            if (dbo != NULL)
                dbo->type = ADD;
        } else if (strncmp(query_command, "sub", 3) == 0){
            query_command += 3;
            dbo = parse_binary_op(query_command, context, handle);
            if (dbo != NULL)
                dbo->type = SUB;
        } else if (strncmp(query_command, "join", 4) == 0){
            query_command += 4;
            dbo = parse_join(query_command, context, handle);
        } else if (strncmp(query_command, "batch_queries()", 15) == 0){
            dbo = malloc(sizeof(DbOperator));
            dbo->type = BATCH_QUERIES;
        } else if (strncmp(query_command, "batch_execute()", 16) == 0){
            dbo = malloc(sizeof(DbOperator));
            dbo->type = BATCH_EXECUTE;
        }
    }
    

    if(dbo == NULL && context->incoming_load == false){
       send_message->status = INCORRECT_FORMAT;
       log_info( "query: %s is INCORRECT_FORMAT \n", query_command);
       return NULL;
    }
    // this is the case for load ops, which send two sets of messages.
    if (dbo == NULL){
        printf("2incoming load %d\n ", context->incoming_load);
        return NULL;
    }

    printf("incoming load %d\n ", context->incoming_load);
    send_message->status = OK_DONE;
    dbo->client_fd = client_socket;
    dbo->context = context;
   
    
    return dbo;
}
