#include "client_context.h"

#include <string.h>

#include "utils.h"

/*
 * Utility functions to find tables and columns based on name
 * Uses linear search, could also use a hashtable, but that may be overkill.
 * returns null if none found
 */

// extern Db* g_db;

Table* lookup_table(char* table_name) {
  if (g_db == NULL || table_name == NULL) {
    if (g_db == NULL) {
      log_err("g_db is null in lookup table\n");
    }
    return NULL;
  }
  for (size_t i = 0; i < g_db->tables_size; i++) {
    log_err("table name in db %s table name to match %s\n",
            g_db->tables[i].name, table_name);
    if (strcmp(g_db->tables[i].name, table_name) == 0) return &g_db->tables[i];
  }
  return NULL;
}

Column* lookup_column(Table* table, char* col_name) {
  if (g_db == NULL || table == NULL || col_name == NULL) return NULL;
  for (size_t i = 0; i < table->col_count; i++) {
    log_err("col name in lookup col %s col name to match %s\n",
            table->columns[i].name, col_name);
    if (strcmp(table->columns[i].name, col_name) == 0)
      return &table->columns[i];
  }
  return NULL;
}

GeneralizedColumnHandle* lookup_context(char* handle, ClientContext* context) {
  for (size_t i = 0; i < context->chandles_in_use; i++) {
    if (strcmp(context->chandle_table[i].name, handle) == 0)
      return &context->chandle_table[i];
  }
  return NULL;
}

/*
 * This function returns a generalized column given a handle.
 * The handle may be a handle in the users context or a fully qualified column
 * name.
 */

GeneralizedColumn* lookup_src(char* handle, ClientContext* context) {
  GeneralizedColumn* src_generalized_col = malloc(sizeof(GeneralizedColumn));
  if (src_generalized_col == NULL) {
    log_err("%s:%d Failed to malloc \n", __FILE__, __LINE__);
  }
  // dont need db name
  char* handle_if_not_column = split_table_column(&handle);
  char* table_name = split_table_column(&handle);
  log_err("table name %s  handle name %s\n", table_name, handle);
  Table* src_table = lookup_table(table_name);
  Column* src_column = lookup_column(src_table, handle);
  log_err("src table name src %s  column name %s\n", src_table->name,
          src_column->name);
  if (src_column != NULL) {
    // yes this is a bandaid. Somewhere, somehow the src_column->num_rows
    // pointer gets redirected to the wrong table when I have time I will find
    // how.
    src_column->num_rows = &src_table->table_length;
    src_generalized_col->column_pointer.column = src_column;
    src_generalized_col->column_type = COLUMN;
  } else {
    GeneralizedColumnHandle* generalized_src_handle =
        lookup_context(handle_if_not_column, context);
    if (generalized_src_handle == NULL) {
      return NULL;
    }
    *src_generalized_col = generalized_src_handle->generalized_column;
  }
  return src_generalized_col;
}

bool insert_result_context(Result* result, char* handle_name,
                           ClientContext* context) {
  GeneralizedColumnHandle* handle_ptr = lookup_context(handle_name, context);
  if (handle_ptr != NULL) {
    if (handle_ptr->generalized_column.column_type == RESULT) {
      free(handle_ptr->generalized_column.column_pointer.result);
    }
    handle_ptr->generalized_column.column_type = RESULT;
    handle_ptr->generalized_column.column_pointer.result = result;
  } else {
    // handle_ptr = calloc(1, sizeof(GeneralizedColumnHandle));
    // strcpy(handle_ptr->name, handle_name);
    // handle_ptr->generalized_column.column_pointer.result = result;

    // check that there is space in the chandle table and realloc if needed
    if (context->chandles_in_use == context->chandle_slots) {
      context->chandle_table =
          realloc(context->chandle_table, 2 * context->chandles_in_use *
                                              sizeof(GeneralizedColumnHandle));
      context->chandle_slots = 2 * context->chandles_in_use;
    }
    context->chandle_table[context->chandles_in_use]
        .generalized_column.column_pointer.result = result;  // = *handle_ptr;
    context->chandle_table[context->chandles_in_use]
        .generalized_column.column_type = RESULT;
    strcpy(context->chandle_table[context->chandles_in_use].name, handle_name);
    context->chandles_in_use += 1;
    // free(handle_ptr);
  }
  return true;
}

/*
 * Iterate through and free the payload of each result
 */

bool free_result_context(ClientContext* context) {
  for (size_t i = 0; i < context->chandles_in_use; i++) {
    if (context->chandle_table[i].generalized_column.column_type == RESULT) {
      free(context->chandle_table[i]
               .generalized_column.column_pointer.result->payload);
      free(context->chandle_table[i].generalized_column.column_pointer.result);
    }
  }
  return true;
}
