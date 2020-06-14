// This file includes shared constants and other values.
#ifndef COMMON_H__
#define COMMON_H__
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
// define the socket path if not defined. 
// note on windows we want this to be written to a docker container-only path
#ifndef SOCK_PATH
#define SOCK_PATH "/tmp/unix_socket_local"
#endif

#endif  // COMMON_H__