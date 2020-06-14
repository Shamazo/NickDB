#ifndef PERSIST_H
#define PERSIST_H

#include "main_api.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>

#define DATA_PATH "/db/data/" //for docker
#define LEN_DATA_PATH 12// for docker

bool startup_db();
bool write_db();
int create_dir(const char* path);

bool save_column_data(Column *col, size_t tables_size);
bool load_column_data(Column *col, size_t tables_size, char* table_name);

bool save_index(Column* column, char* table_name);
bool load_index(Column* column, char* table_name);


#endif