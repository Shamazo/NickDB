/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/sendfile.h>


#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}




/*
* Needs to create a bulk load message to send
* The format is """load(db.table,num_columns,num_rows)\n
column1 data \n
column2 data \n
*/

message* handle_load(char* path){
    //open file
    path += 5;
    path = trim_whitespace(path);
    path = trim_quotes(path);
    path = trim_parenthesis(path);

    // open file
    FILE* fp = fopen(path, "r");


    char buf[1024];
    // get column length / num lines
    size_t num_rows = 0;

    if ( fp == NULL ) {
        return NULL;
    }

    for (char c = getc(fp); c != EOF; c = getc(fp)){
        if (c == '\n'){
            num_rows += 1;
        }
    }
    // subtract header row
    num_rows -=1;
    fseek(fp, 0, SEEK_SET);
    // read first row
    fgets(buf, sizeof(buf), fp);

    //make a copy of buffer, iterate through to see how many columns we have
    char* buf_copy = malloc(sizeof(char) * 1024);
    char* to_free_2 = buf_copy;
    strcpy(buf_copy, buf);
    size_t num_cols = 0;
    while (true) {
        char* arg = strsep(&buf_copy, ",");
        if (arg == NULL){
            break;
        }
        num_cols += 1;
    }
    free(to_free_2);
    char* first_col_name = malloc((strlen(buf) + 1) * sizeof(char));
    char* to_free = first_col_name;
    strcpy(first_col_name, buf);
    first_col_name = strsep(&first_col_name, ",");
    // drop db name since we dont need it
    char* db_name = strsep(&first_col_name, ".");
    char* tbl_name = strsep(&first_col_name, ".");

    size_t message_len = 2*MAX_SIZE_NAME*num_cols + num_cols * num_rows * 100;
    char* message_buf = malloc(message_len * sizeof(char));
    size_t message_pos = 0;
    message_pos += sprintf(message_buf, "load(%s.%s,%ld,%ld)", db_name, tbl_name, num_cols, num_rows);
    message_buf[message_pos] = '\n';
    message_pos += 1;

    int** values= malloc(sizeof(int) * num_cols);
    for (size_t i=0; i < num_cols; i++){
        values[i] = malloc(sizeof(int)*num_rows);
    }

    for (size_t i=0; i < num_rows; i++){
        fgets(buf, sizeof(buf), fp);
        buf_copy = malloc(sizeof(char) * 1024);
        char* to_free_3 = buf_copy;
        strcpy(buf_copy, buf);
        // printf(" reading buf line %s\n", buf );
        size_t j = 0;
        while (true) {
            char* val = strsep(&buf_copy, ",");
            // printf(" putting str in values %s\n", val);
            if (val == NULL){
                break;
            }
            int int_val = atoi(val);
            // printf(" putting int in values %d\n", int_val);
            (values[j])[i] = int_val;
            j += 1;
        }
        free(to_free_3);
    }


    for (size_t i=0; i<num_cols; i++){
        for (size_t j=0; j<num_rows; j++){
            // printf(" putting in buf %d\n", (values[i])[j]);
            message_pos += sprintf(&message_buf[message_pos], "%d,", (values[i])[j]);
        }
        message_buf[message_pos] = '\n';
        message_pos+=1;
    }
    message_buf[message_pos] = '\0';


    free(to_free);
    // printf("message_buf %s\n", message_buf );
    message* new_message = malloc(sizeof(message));
    new_message->length = strlen(message_buf)+1;
    new_message->status = 0;
    new_message->payload = message_buf;
    return new_message;
}


file_struct open_file(char* path){
    //open file
    path += 5;
    path = trim_whitespace(path);
    path = trim_quotes(path);
    path = trim_parenthesis(path);

    // open file
    int fd = open(path, O_RDONLY);

    struct stat st;
    fstat(fd, &st);
    struct file_struct file;
    file.length = st.st_size;
    file.fd = fd;
    return file;

}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *
**/
int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    //worry about default buffer size
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
                                                    DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            // handle load messages differently from the rest
            if (strncmp(read_buffer, "load", 4) == 0) {
                // message* new_message = handle_load(read_buffer);
                file_struct csv = open_file(read_buffer);
                // strcpy(send_message.payload, new_payload);
                // send_message.length = strlen(send_message.payload);
                if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
                    log_err("Failed to send message header.");
                    exit(1);
                }
                if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                    log_err("Failed to send load payload.");
                    exit(1);
                }

                // wait for server response to first set of messages
                if ((len = recv(client_socket, &(recv_message), sizeof(message), MSG_WAITALL)) > 0) {
                    if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                        (int) recv_message.length > 0) {
                        // Calculate number of bytes in response package
                        int num_bytes = (int) recv_message.length;
                        char* payload = malloc(num_bytes + 1);

                        // Receive the payload and print it out
                        if ((len = recv(client_socket, payload, num_bytes,  MSG_WAITALL)) > 0) {
                            payload[num_bytes] = '\0';
                            printf("%s\n", payload);
                        }
                        free(payload);
                    }
                }

                send_message.length = csv.length;
                if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
                    log_err("Failed to send load message header.");
                    exit(1);
                }
                sendfile(client_socket, csv.fd, 0, csv.length);

            } else {
                // Send the message_header, which tells server payload size
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message header.");
                    exit(1);
                }

                // Send the payload (query) to server
                if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                    log_err("Failed to send query payload.");
                    exit(1);
                }
            }




            // Always wait for server response (even if it is just an OK message)
            if ((len = recv(client_socket, &(recv_message), sizeof(message), MSG_WAITALL)) > 0) {
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char* payload = malloc(num_bytes + 1);

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes,  MSG_WAITALL)) > 0) {
                        payload[num_bytes] = '\0';
                        printf("%s\n", payload);
                    }
                    free(payload);
                }
            }
            else {
                if (len < 0) {
                    log_err("Failed to receive message.");
                }
                else {
                    log_info("-- Server closed connection\n");
                }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}