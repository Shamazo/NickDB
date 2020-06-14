#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "main_api.h"

Table* lookup_table(char *name);
Column* lookup_column(Table *table, char *col_name);
GeneralizedColumnHandle* lookup_context(char* handle,  ClientContext* context);
GeneralizedColumn* lookup_src(char* handle,  ClientContext* context);
bool insert_result_context(Result* result, char* handle_name, ClientContext* context);
bool free_result_context(ClientContext* context);
// GeneralizedColumnHandle* lookup_handle(char *name);

#endif
