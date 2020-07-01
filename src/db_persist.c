/** db_persist.c
 *
 * This file contains functions to load the databse catalogue if it exists
 **/

#include "db_persist.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common.h"
#include "db_index.h"
#include "main_api.h"
#include "message.h"
#include "parse.h"
#include "utils.h"

extern Db* g_db;

bool startup_db() {
  // Make the data directory, checking if it already exists
  if (mkdir(DATA_PATH, S_IRUSR | S_IWUSR) < 0) {
    if (errno == EEXIST) {
      log_info("Data directory already exists \n");
    } else {
      log_err("Error making data directory: %s\n", strerror(errno));
    }
  } else {
    log_info("Created data directory. \n");
  }

  char* cat_name = "catalogue.cat";
  int path_length = LEN_DATA_PATH + strlen(cat_name) + 1;
  char cat_file_path[path_length];
  sprintf(cat_file_path, "%s%s", DATA_PATH, cat_name);
  FILE* cat_file = fopen(cat_file_path, "r");
  // If not catalogue exists then there is no database and we are done
  if (cat_file == NULL) {
    log_info("No database catalogue file found\n");
    return true;
  } else {
    log_info("Attempting to load catalogue file\n");
  }

  g_db = calloc(1, sizeof(Db));
  int read_bytes = fread(g_db, sizeof(Db), 1, cat_file);
  if (ferror(cat_file)) {
    log_err("Failed to read catalogue, read %d items, errno %d, %s \n",
            read_bytes, errno, strerror(errno));
    return false;
  }

  size_t num_tables_to_read = g_db->tables_size;
  g_db->tables = calloc(1, sizeof(Table) * num_tables_to_read);
  g_db->tables_capacity = g_db->tables_size;

  // We read in the catalogue in the format described in write_db() below.
  Table* current_table = malloc(sizeof(Table));
  for (size_t i = 0; i < num_tables_to_read; i++) {
    if (fread(current_table, sizeof(Table), 1, cat_file) < 1) {
      log_err("%s:%d Failed to read table, read 0 items \n", __FILE__,
              __LINE__);
    };
    size_t num_columns_to_read = current_table->col_count;
    current_table->columns = malloc(sizeof(Column) * num_columns_to_read);

    for (size_t j = 0; j < num_columns_to_read; j++) {
      if (fread(&current_table->columns[j], sizeof(Column), 1, cat_file) < 1) {
        log_err("%s:%d Failed to read column, read 0 items \n", __FILE__,
                __LINE__);
      }
      load_column_data(&current_table->columns[j],
                       current_table->table_alloc_size, current_table->name);
      current_table->columns[j].num_rows = &current_table->table_length;
      load_index(&current_table->columns[j], current_table->name);
      current_table->columns[j].update_struct.alloc_size = UPDATE_BATCH_SIZE;
      current_table->columns[j].update_struct.del_pos =
          malloc(sizeof(size_t) * UPDATE_BATCH_SIZE);
      current_table->columns[j].update_struct.del_val =
          malloc(sizeof(int) * UPDATE_BATCH_SIZE);
      current_table->columns[j].update_struct.ins_val =
          malloc(sizeof(int) * UPDATE_BATCH_SIZE);
      current_table->columns[j].update_struct.del_length = 0;
      current_table->columns[j].update_struct.ins_length = 0;
    }
    g_db->tables[i] = *current_table;
  }
  fclose(cat_file);
  return true;
}

/*
 * This function write the database to disk. This happens in two parts. First we
 * write the db, tb and column structs to catalogue.cat This is in the format
 * [db][tb1][tb1.cola][tb1.colb][tb2][tb2.cola], i.e the db struct followed by
 * each table and all of that tables associated columns. This is done because
 * while reading we first find how many tables there are and then for each table
 * we find how many columns follow it before the next table. should I also free
 * memory here? yeah probs
 */

bool write_db() {
  char* cat_name = "catalogue.cat";
  int path_length = LEN_DATA_PATH + strlen(cat_name) + 1;
  char cat_file_path[path_length];
  sprintf(cat_file_path, "%s%s", DATA_PATH, cat_name);
  FILE* cat_file = fopen(cat_file_path, "w");
  if (cat_file == NULL) {
    log_err("Failed to open catalogue file for writing\n");
    return true;
  }
  // For debugging purposes, should remove this at some point as it should be
  // possible to start the program and quit without creating a db and that
  // shouldn't fail
  assert(g_db != NULL);
  fwrite(g_db, sizeof(Db), 1, cat_file);

  size_t num_tables_to_write = g_db->tables_size;

  for (size_t i = 0; i < num_tables_to_write; i++) {
    Table* current_table = &g_db->tables[i];
    if (!flush_updates(current_table)) {
      log_err("Failed to flush updates for table %s\n", current_table->name);
    }
    if (!free_update_structure(current_table)) {
      log_err("Failed to free update structures for table %s\n",
              current_table->name);
    }
    fwrite(current_table, sizeof(Table), 1, cat_file);
    size_t num_columns_to_write = current_table->col_count;

    for (size_t j = 0; j < num_columns_to_write; j++) {
      save_column_data(&current_table->columns[j],
                       current_table->table_alloc_size);
      if (current_table->columns[j].index_type != NONE) {
        save_index(&current_table->columns[j], current_table->name);
      }
      fwrite(&current_table->columns[j], sizeof(Column), 1, cat_file);
    }
    free(current_table->columns);
  }
  free(g_db->tables);
  free(g_db);

  fclose(cat_file);

  return true;
}
/*
 * This function flushes the col memmap to disk and closes the associated fd.
 * don't need to worry about the fd since that is dealt with in column creation
 * or loading.
 */

bool save_column_data(Column* col, size_t tables_size) {
  // could do this async but this isn't performance critical and may cause
  // problems
  msync(col->data, tables_size, MS_SYNC);
  if (munmap(col->data, sizeof(int) * tables_size) == -1) {
    log_err("failed to munmap column %s, errno %d, %s \n", col->name, errno,
            strerror(errno));
    return false;
  }
  // printf(" before save return \n");
  return true;
}

bool load_column_data(Column* col, size_t tables_size, char* table_name) {
  int path_length =
      LEN_DATA_PATH + strlen(table_name) + strlen(col->name) + 4 + 2;

  char col_file_path[path_length];
  sprintf(col_file_path, "%s%s/%s%s", DATA_PATH, table_name, col->name, ".col");
  int fd = open(col_file_path, O_RDWR);
  if (fd == -1) {
    log_err("Failed to open column: %s for writing, errno: %d \n", col->name,
            errno);
    return false;
  }

  col->data = mmap(NULL, tables_size * sizeof(int), PROT_WRITE | PROT_READ,
                   MAP_SHARED, fd, 0);
  if (col->data == MAP_FAILED) {
    log_err("%s:%d Failed to mmap column, errno: %d , strerror: %s\n", __FILE__,
            __LINE__, errno, strerror(errno));
    return false;
  }

  close(fd);
  return true;
}

/*
 * Funtion to save each node to disk during BFS traversal
 */

void save_node_to_file(BNode* node, FILE* out_file) {
  // printf("Call save_node_to_file \n");

  fwrite(node, sizeof(BNode), 1, out_file);
  return;
}

bool save_index(Column* column,
                char* table_name) {  // void* index, IndexType index_type, char*
                                     // table_name, char* col_name){
  int path_length =
      LEN_DATA_PATH + strlen(table_name) + strlen(column->name) + 4 + 2;
  char col_index_path[path_length];
  sprintf(col_index_path, "%s%s/%s%s", DATA_PATH, table_name, column->name,
          ".index");
  FILE* index_file = fopen(col_index_path, "w+");
  if (column->index_type == SORTED) {
    SortedIndex* sorted_index = (SortedIndex*)column->index;
    fwrite(sorted_index, sizeof(SortedIndex), 1, index_file);
    if (column->clustered == false) {
      fwrite(sorted_index->keys, sizeof(int), sorted_index->length, index_file);
    }
    fwrite(sorted_index->col_positions, sizeof(size_t), sorted_index->length,
           index_file);
  }

  if (column->index_type == BTREE) {
    BNode* root = (BNode*)column->index;
    // printf("Calling trasverse tree with save node to file \n");
    traverse_tree(root, save_node_to_file, index_file);
  }
  return true;
}

/*
 * Since the nodes are saved in BFS order, we need to do the opposite
 */

bool load_btree_index(Column* column, FILE* in_file) {
  struct stat buf;
  int fd = fileno(in_file);
  fstat(fd, &buf);
  off_t size = buf.st_size;
  size_t num_nodes = size / sizeof(BNode);

  // Unlike the BFS traversal, we easily know the number of nodes based on the
  // size of the file.
  BNode** all_node_ptrs = malloc(sizeof(BNode*) * num_nodes);

  BNode* root = malloc(sizeof(BNode));
  if (fread(root, sizeof(BNode), 1, in_file) < 1) {
    log_err("%s:%d Failed to read head of btree %d %s \n", __FILE__, __LINE__,
            errno, strerror(errno));
    return false;
  }
  column->index = root;
  all_node_ptrs[0] = root;

  size_t seen_nodes = 1;
  size_t curr_idx = 0;

  BNode* prev_leaf = NULL;

  while (curr_idx < seen_nodes) {
    printf("\n");
    // if (seen_nodes + 80 > num_nodes){
    //      all_node_ptrs = realloc(all_node_ptrs, sizeof(BNode*) * (num_nodes +
    //      81)); num_nodes += 81;
    // }
    // printf("num_nodes %ld\n", num_nodes);
    // printf("curr_idx %ld, seen_nodes %ld \n", curr_idx, seen_nodes);
    BNode* curr_node = all_node_ptrs[curr_idx];
    // print_node(curr_node, NULL);

    if (curr_node->is_leaf == false) {
      if (curr_node == NULL) {
        curr_idx += 1;
        // printf("curr_node is NULL \n");
        continue;
      }
      // print_node(curr_node, NULL);

      size_t ptrs_in_node =
          curr_node->num_elements;  // +1 is the rightmost edge pointer
      // if (curr_node->children.child_pointers[ptrs_in_node] == NULL){
      //     ptrs_in_node -= 1;
      // }
      size_t read_nodes = 0;
      // printf("ptrs_in_node %ld \n", ptrs_in_node );

      for (size_t i = 0; i < ptrs_in_node; i++) {
        BNode* in_node = malloc(sizeof(BNode));
        read_nodes += fread(in_node, sizeof(BNode), 1, in_file);
        curr_node->children.child_pointers[i] = in_node;
        all_node_ptrs[seen_nodes] = in_node;
        seen_nodes += 1;
      }

      // If we are end the end of the file
      // if (read_nodes != ptrs_in_node) {
      //     log_err("%s:%d Failed to read all children of node  readnodes:%ld
      //     ptrs_in_node: %ld. errno %d strerror %s\n", __FILE__, __LINE__,
      //     read_nodes, ptrs_in_node, errno, strerror(errno));
      //     // return false;
      //     break;
      // }

      // we only keep track of next/prev nodes on the leaf level
    } else {
      if (prev_leaf == NULL) {
        prev_leaf = curr_node;
      } else {
        prev_leaf->next = curr_node;
        curr_node->previous = prev_leaf;
        prev_leaf = curr_node;
      }
    }
    curr_idx += 1;
  }

  return true;
}

bool load_sorted_index(Column* column, FILE* in_file) {
  SortedIndex* sorted_index = malloc(sizeof(SortedIndex));
  if (fread(sorted_index, sizeof(SortedIndex), 1, in_file) < 1) {
    log_err("%s:%d Failed to read SortedIndex struct %s\n", __FILE__, __LINE__,
            errno, strerror(errno));
    return false;
  }
  // printf("loaded struct reading %ld entries %ld allocated_size \n",
  // sorted_index->length, sorted_index->allocated_size);

  sorted_index->keys = malloc(sizeof(int) * sorted_index->allocated_size);
  if (column->clustered == false) {
    if (fread(sorted_index->keys, sizeof(int), sorted_index->length, in_file) <
        sorted_index->length) {
      log_err("%s:%d Failed to read sorted_index keys %s\n", __FILE__, __LINE__,
              errno, strerror(errno));
      return false;
    }
  } else {
    sorted_index->keys = column->data;
  }

  sorted_index->col_positions =
      malloc(sizeof(size_t) * sorted_index->allocated_size);
  if (fread(sorted_index->col_positions, sizeof(size_t), sorted_index->length,
            in_file) < sorted_index->length) {
    log_err("%s:%d Failed to read sorted_index col_positions %s\n", __FILE__,
            __LINE__, errno, strerror(errno));
    return false;
  }

  column->index = sorted_index;
  return true;
}

bool load_index(Column* column, char* table_name) {
  if (column->index_type == NONE) {
    return true;
  }

  int path_length =
      LEN_DATA_PATH + strlen(table_name) + strlen(column->name) + 4 + 2;
  char col_index_path[path_length];
  sprintf(col_index_path, "%s%s/%s%s", DATA_PATH, table_name, column->name,
          ".index");
  FILE* index_file = fopen(col_index_path, "r+");
  if (column->index_type == BTREE) {
    bool res = load_btree_index(column, index_file);
    fclose(index_file);
    return res;
  } else if (column->index_type == SORTED) {
    bool res = load_sorted_index(column, index_file);
    fclose(index_file);
    return res;
  }
  return false;
}