/** server.c
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "client_context.h"
#include "common.h"
#include "db_index.h"
#include "db_persist.h"
#include "main_api.h"
#include "message.h"
#include "parse.h"
#include "utils.h"
#define DEFAULT_QUERY_BUFFER_SIZE 1024

#define DEBUG true

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
  int done = 0;
  int length = 0;

  log_info("Connected to socket: %d.\n", client_socket);

  // Create two messages, one from which to read and one from which to receive
  message send_message;
  message recv_message;

  // create the client context here
  ClientContext* client_context = malloc(sizeof(ClientContext));
  client_context->chandle_table = malloc(sizeof(GeneralizedColumnHandle) * 10);
  client_context->chandles_in_use = 0;
  client_context->chandle_slots = 10;
  client_context->batching_active = false;
  client_context->incoming_load = false;
  client_context->batch_operators = NULL;
  client_context->batch_operators_in_use = 0;
  client_context->batch_operator_slots = 0;

  bool free_result = false;
  bool shutdown = false;
  // Continually receive messages from client and execute queries.
  // 1. Parse the command
  // 2. Handle request if appropriate
  // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
  // 4. Send response to the request.
  char* recv_buffer = NULL;
  char* send_buffer = NULL;

  do {
    length = recv(client_socket, &recv_message, sizeof(message), 0);
    if (length < 0) {
      log_err("Client connection closed!\n");
      exit(1);
    } else if (length == 0) {
      done = 1;
    }

    if (!done) {
      recv_buffer = realloc(recv_buffer, recv_message.length + 1);
      length =
          recv(client_socket, recv_buffer, recv_message.length, MSG_WAITALL);
      recv_message.payload = recv_buffer;
      recv_message.payload[recv_message.length] = '\0';

      printf("recieved message %s\n", recv_message.payload);

      // 1. Parse command
      //    Query string is converted into a request for an database operator
      DbOperator* query = parse_command(recv_message.payload, &send_message,
                                        client_socket, client_context);
      // Only print and load queries use heap memory for results
      if (query != NULL && (query->type == PRINT)) {
        free_result = true;
      }
      if (query != NULL && query->type == SHUTDOWN) {
        shutdown = true;
      }

      // 2. Handle request
      //    Corresponding database operator is executed over the query
      char* result;
      if (query != NULL) {
        result = execute_DbOperator(query, client_context);
      } else {
        result = " ";
      }

      send_message.length = strlen(result);
      send_buffer = realloc(send_buffer, send_message.length + 1);
      strcpy(send_buffer, result);
      send_message.payload = send_buffer;
      send_message.status = OK_WAIT_FOR_RESPONSE;

      // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
      if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
      }
      printf("sent status \n");

      // 4. Send response to the request
      if (send(client_socket, result, send_message.length, 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
      }
      printf("sent result \n");

      if (free_result) {
        free(result);
        free_result = false;
      }

      if (shutdown == true) {
        done = 1;
      }
    }
  } while (!done);

  log_info("Connection closed at socket %d!\n", client_socket);
  free_result_context(client_context);
  free(client_context->chandle_table);
  free(client_context);
  close(client_socket);
  free(recv_buffer);
  free(send_buffer);
  if (shutdown) {
    exit(0);
  }
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
  int server_socket;
  size_t len;
  struct sockaddr_un local;

  log_info("Attempting to setup server...\n");

  if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  local.sun_family = AF_UNIX;
  strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  unlink(local.sun_path);

  int on = 1;
  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char*)&on,
                 sizeof(on)) < 0) {
    log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
    return -1;
  }

  len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
  if (bind(server_socket, (struct sockaddr*)&local, len) == -1) {
    log_err("L%d: Socket failed to bind.\n", __LINE__);
    log_err("Error code: %d\n", errno);
    return -1;
  }

  if (listen(server_socket, 5) == -1) {
    log_err("L%d: Failed to listen on socket.\n", __LINE__);
    printf("Error code: %d\n", errno);
    return -1;
  }

  return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
//
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will
//      allow? What aspects of siloes or isolation are maintained in your
//      design? (Think `what` is shared between `whom`?)
int main(void) {
  // this is a main memory db, this locks everything to be in memory
  // especially the mmapped columns
  mlockall(MCL_FUTURE);
  // printf(" size of bnode %ld\n",sizeof(BNode) );

  // printf("This system has %d processors configured and "
  //     "%d processors available.\n",
  //     get_nprocs_conf(), get_nprocs());
  // printf("Cache line size of %ld\n",  sysconf (_SC_LEVEL1_DCACHE_LINESIZE));

  // FILE *fp = fopen("/proc/cpuinfo", "r");
  //     size_t n = 0;
  //     char *line = NULL;
  //     while (getline(&line, &n, fp) > 0) {
  //         printf("%s", line);
  //     }
  //     free(line);

  if (!startup_db()) {
    log_err("Failed to load database from disk\n");
    exit(1);
  }

  int server_socket = setup_server();
  if (server_socket < 0) {
    exit(1);
  }

  while (true) {
    log_info("Waiting for a connection %d ...\n", server_socket);
    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket =
             accept(server_socket, (struct sockaddr*)&remote, &t)) == -1) {
      log_err("L%d: Failed to accept a new connection.\n", __LINE__);
      exit(1);
    }
    handle_client(client_socket);
  }

  if (write_db()) {
    return 0;
  } else {
    log_err("Failed to write database to disk\n");
    return 0;
  }
}
