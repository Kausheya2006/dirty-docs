#include "utils.h"
#include <stdarg.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/select.h>

// Global mutex for thread-safe logging
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void die(char *s) {
    perror(s);
    exit(1);
}

int create_server_socket(int port) {
    int listen_fd;
    struct sockaddr_in serv_addr;

    if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        die("ERROR opening socket");
    }

    // Allow address reuse
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        die("ERROR on setsockopt");
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        die("ERROR on binding");
    }

    if (listen(listen_fd, 5) < 0) {
        die("ERROR on listen");
    }
    
    return listen_fd;
}

int connect_to_server(const char* ip, int port) {
    int sock_fd;
    struct sockaddr_in serv_addr;

    if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        die("ERROR opening socket");
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        die("ERROR invalid address");
    }

    if (connect(sock_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        die("ERROR on connecting");
    }

    return sock_fd;
}

// Safe connect with timeout that returns -1 on failure instead of dying
int connect_to_server_timeout(const char* ip, int port, int timeout_sec) {
    int sock_fd;
    struct sockaddr_in serv_addr;

    if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return -1;
    }

    // Set socket to non-blocking
    int flags = fcntl(sock_fd, F_GETFL, 0);
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &serv_addr.sin_addr) <= 0) {
        close(sock_fd);
        return -1;
    }

    int res = connect(sock_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
    if (res < 0) {
        if (errno == EINPROGRESS) {
            // Connection in progress, wait with timeout
            fd_set fdset;
            struct timeval tv;
            FD_ZERO(&fdset);
            FD_SET(sock_fd, &fdset);
            tv.tv_sec = timeout_sec;
            tv.tv_usec = 0;

            if (select(sock_fd + 1, NULL, &fdset, NULL, &tv) > 0) {
                int so_error;
                socklen_t len = sizeof(so_error);
                getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
                if (so_error == 0) {
                    // Connection successful, set back to blocking
                    fcntl(sock_fd, F_SETFL, flags);
                    return sock_fd;
                }
            }
        }
        close(sock_fd);
        return -1;
    }

    // Connected immediately, set back to blocking
    fcntl(sock_fd, F_SETFL, flags);
    return sock_fd;
}

// Initialize log file (create if doesn't exist)
void init_log_file(const char* log_file_path) {
    // Extract directory path from log_file_path
    char dir_path[256];
    strncpy(dir_path, log_file_path, sizeof(dir_path) - 1);
    dir_path[sizeof(dir_path) - 1] = '\0';
    
    // Find the last '/' to get directory path
    char* last_slash = strrchr(dir_path, '/');
    if (last_slash != NULL) {
        *last_slash = '\0'; // Terminate at the last slash
        
        // Create directory if it doesn't exist (mkdir -p behavior)
        struct stat st = {0};
        if (stat(dir_path, &st) == -1) {
            if (mkdir(dir_path, 0755) == -1) {
                fprintf(stderr, "WARNING: Could not create log directory %s: %s\n", 
                       dir_path, strerror(errno));
                // Try to continue anyway
            }
        }
    }
    
    FILE* file = fopen(log_file_path, "a");
    if (!file) {
        fprintf(stderr, "WARNING: Could not open log file %s: %s\n", 
               log_file_path, strerror(errno));
        return;
    }
    
    // Write header if file is new/empty
    fseek(file, 0, SEEK_END);
    if (ftell(file) == 0) {
        fprintf(file, "=== Log Started ===\n");
    }
    fclose(file);
}

// Get current timestamp in readable format
void get_timestamp(char* buffer, size_t size) {
    time_t now = time(NULL);
    struct tm* tm_info = localtime(&now);
    strftime(buffer, size, "%Y-%m-%d %H:%M:%S", tm_info);
}

// Get client IP and port from socket
void get_client_info(int sock_fd, char* ip_buffer, int* port) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    
    if (getpeername(sock_fd, (struct sockaddr*)&addr, &addr_len) == 0) {
        inet_ntop(AF_INET, &addr.sin_addr, ip_buffer, INET_ADDRSTRLEN);
        *port = ntohs(addr.sin_port);
    } else {
        strcpy(ip_buffer, "unknown");
        *port = 0;
    }
}

// Thread-safe logging function
void log_message(const char* log_file_path, const char* level, const char* format, ...) {
    pthread_mutex_lock(&log_mutex);
    
    char timestamp[64];
    get_timestamp(timestamp, sizeof(timestamp));
    
    // Print to terminal with colors
    const char* color = "";
    const char* reset = "\033[0m";
    if (strcmp(level, "INFO") == 0) {
        color = "\033[0;36m";  // Cyan
    } else if (strcmp(level, "SUCCESS") == 0) {
        color = "\033[0;32m";  // Green
    } else if (strcmp(level, "WARNING") == 0) {
        color = "\033[0;33m";  // Yellow
    } else if (strcmp(level, "ERROR") == 0) {
        color = "\033[0;31m";  // Red
    } else if (strcmp(level, "REQUEST") == 0) {
        color = "\033[0;35m";  // Magenta
    } else if (strcmp(level, "RESPONSE") == 0) {
        color = "\033[0;34m";  // Blue
    }
    
    // Print to terminal
    printf("%s[%s]%s [%s] ", color, level, reset, timestamp);
    va_list args1;
    va_start(args1, format);
    vprintf(format, args1);
    va_end(args1);
    printf("\n");
    fflush(stdout);
    
    // Append to log file
    FILE* file = fopen(log_file_path, "a");
    if (file) {
        fprintf(file, "[%s] [%s] ", level, timestamp);
        va_list args2;
        va_start(args2, format);
        vfprintf(file, format, args2);
        va_end(args2);
        fprintf(file, "\n");
        fclose(file);
    }
    
    pthread_mutex_unlock(&log_mutex);
}