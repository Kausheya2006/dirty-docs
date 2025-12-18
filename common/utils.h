#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

// A simple error handler
void die(char *s);

// Creates and returns a listening socket FD for a server
int create_server_socket(int port);

// Creates and returns a connected socket FD for a client
int connect_to_server(const char* ip, int port);

// Safe connect with timeout (returns -1 on failure, doesn't exit)
int connect_to_server_timeout(const char* ip, int port, int timeout_sec);

// Logging functions
void init_log_file(const char* log_file_path);
void log_message(const char* log_file_path, const char* level, const char* format, ...);
void get_timestamp(char* buffer, size_t size);
void get_client_info(int sock_fd, char* ip_buffer, int* port);

#endif