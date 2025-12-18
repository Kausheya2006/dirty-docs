#include "../common/utils.h"
#include "../common/config.h"
#include <sys/stat.h> // For mkdir
#include <errno.h>
#include "../name_server/ns_utils.h"
#include <fcntl.h>
#include "ss_utils.h"

char SS_DATA_DIR[100]; // Global to store this SS's data directory (e.g., "ss1_data/")
char SS_ID[50]; // Global to store this SS's ID
char SS_LOG_FILE[150]; // Log file path

// Helper to extract base filename from path
char* get_base_filename_ss(const char* path) {
    const char* last_slash = strrchr(path, '/');
    if (last_slash == NULL) {
        return (char*)path; // No slash, it's already a base filename
    }
    return (char*)(last_slash + 1); // Return the part after the slash
}

// --- Handler for Client Connections (Phase 4 Placeholder) ---
void *handle_client_connection(void *socket_desc) {
    int sock = *(int*)socket_desc;
    free(socket_desc);
    
    char client_ip[INET_ADDRSTRLEN];
    int client_port;
    get_client_info(sock, client_ip, &client_port);
    log_message(SS_LOG_FILE, "INFO", "Client connected from %s:%d", client_ip, client_port);
    
    char buffer[BUFFER_SIZE];
    int read_size;
    
    // Read the *first* command from the client
    if ((read_size = read(sock, buffer, BUFFER_SIZE)) > 0) {
        buffer[read_size] = '\0';
        log_message(SS_LOG_FILE, "REQUEST", "Received from %s:%d: %s", client_ip, client_port, buffer);
        
        char command[100], filename[MAX_FILENAME];
        int sentence_num;
        sscanf(buffer, "%s %s %d", command, filename, &sentence_num);
        
        char filepath[BUFFER_SIZE];
        snprintf(filepath, sizeof(filepath), "%s/%s", SS_DATA_DIR, filename);

        // --- READ ---
        if (strcmp(command, "READ") == 0) {
            log_message(SS_LOG_FILE, "INFO", "Processing READ request for %s from %s:%d", filename, client_ip, client_port);
            handle_read(sock, filepath);
        }
        // --- STREAM ---
        else if (strcmp(command, "STREAM") == 0) {
            log_message(SS_LOG_FILE, "INFO", "Processing STREAM request for %s from %s:%d", filename, client_ip, client_port);
            handle_stream(sock, filepath);
        }
        // --- WRITE ---
        else if (strcmp(command, "WRITE") == 0) {
            log_message(SS_LOG_FILE, "INFO", "Processing WRITE request for %s (sentence %d) from %s:%d", 
                       filename, sentence_num, client_ip, client_port);
            // handle_write will manage the rest of the connection
            handle_write(sock, filepath, sentence_num);
        }
        else if (strcmp(command, "UNDO") == 0) { 
            log_message(SS_LOG_FILE, "INFO", "Processing UNDO request for %s from %s:%d", filename, client_ip, client_port);
            handle_undo(sock, filepath);
        }
        // --- CHECKPOINT ---
        else if (strcmp(command, "CHECKPOINT") == 0) {
            char tag[128] = {0};
            // buffer format: CHECKPOINT <filename> <tag>
            sscanf(buffer, "%*s %*s %127s", tag);
            handle_checkpoint(sock, filepath, tag);
        }
        // --- VIEWCHECKPOINT ---
        else if (strcmp(command, "VIEWCHECKPOINT") == 0) {
            char tag[128] = {0};
            sscanf(buffer, "%*s %*s %127s", tag);
            handle_viewcheckpoint(sock, filepath, tag);
        }
        // --- LISTCHECKPOINTS ---
        else if (strcmp(command, "LISTCHECKPOINTS") == 0) {
            handle_listcheckpoints(sock, filepath);
        }
        // --- REVERT to CHECKPOINT ---
        else if (strcmp(command, "REVERT") == 0) {
            char tag[128] = {0};
            sscanf(buffer, "%*s %*s %127s", tag);
            handle_revert_to_checkpoint(sock, filepath, tag);
        }
        // --- SHUTDOWN ---
        else if (strcmp(command, "SHUTDOWN") == 0) {
            printf("[SS] Received SHUTDOWN command from Name Server.\n");
            write(sock, "ACK_SHUTDOWN\n", 13);
            close(sock);
            printf("[SS] Storage Server shutting down...\n");
            exit(0);
        }
        else {
            write(sock, "ERR_SS_UNKNOWN_CMD\n", 19);
        }
    }
    
    close(sock);
    printf("[SS-ClientPort] Client connection closed.\n");
    return 0;
}


// --- Listener Thread for Clients ---
void *start_client_listener(void *port_arg) {
    int client_port = *(int*)port_arg;
    free(port_arg);

    int listen_fd = create_server_socket(client_port);
    printf("[SS] Listening for CLIENTS on port %d\n", client_port);

    while (1) {
        int conn_fd = accept(listen_fd, NULL, NULL);
        if (conn_fd < 0) {
            perror("ERROR on client accept");
            continue;
        }
        
        pthread_t thread_id;
        int *new_sock = malloc(sizeof(int));
        *new_sock = conn_fd;
        if (pthread_create(&thread_id, NULL, handle_client_connection, (void*) new_sock) < 0) {
            perror("ERROR creating client handler thread");
            free(new_sock);
        }
        pthread_detach(thread_id);
    }
    close(listen_fd);
    return 0;
}


// --- Handler for Name Server Commands (Phase 3) ---
void *handle_nm_command(void *socket_desc) {
    int sock = *(int*)socket_desc;
    free(socket_desc);
    char buffer[BUFFER_SIZE];
    int read_size;

    char nm_ip[INET_ADDRSTRLEN];
    int nm_port;
    get_client_info(sock, nm_ip, &nm_port);
    
    if ((read_size = read(sock, buffer, BUFFER_SIZE)) > 0) {
        buffer[read_size] = '\0';
        log_message(SS_LOG_FILE, "REQUEST", "Received from NM %s:%d: %s", nm_ip, nm_port, buffer);
        
        char command[100], filename[MAX_FILENAME], arg2[MAX_FILENAME];
        bzero(arg2, MAX_FILENAME);
        sscanf(buffer, "%s %s %s", command, filename, arg2);
        
        char filepath[BUFFER_SIZE];
        snprintf(filepath, sizeof(filepath), "%s/%s", SS_DATA_DIR, filename);

        // --- NM_CREATE ---
        if (strcmp(command, "NM_CREATE") == 0) {
            // Create an empty file
            int fd = open(filepath, O_CREAT | O_WRONLY, 0644);
            if (fd < 0) {
                perror("ERROR creating file");
                log_message(SS_LOG_FILE, "ERROR", "Failed to create file: %s", filepath);
                write(sock, "ERR_NM_CREATE\n", 14);
            } else {
                close(fd);
                write(sock, "ACK_NM_CREATE\n", 14);
                log_message(SS_LOG_FILE, "SUCCESS", "Created file: %s", filepath);
            }
        }
        
        // --- NM_DELETE ---
        else if (strcmp(command, "NM_DELETE") == 0) {
            // Check if file has any active locks before deleting
            if (is_file_locked(filename)) {
                write(sock, "ERR_FILE_LOCKED\n", 16);
                log_message(SS_LOG_FILE, "WARNING", "Cannot delete %s: file is locked", filepath);
            } else if (remove(filepath) == 0) {
                write(sock, "ACK_NM_DELETE\n", 14);
                log_message(SS_LOG_FILE, "SUCCESS", "Deleted file: %s", filepath);
            } else {
                perror("ERROR deleting file");
                log_message(SS_LOG_FILE, "ERROR", "Failed to delete file: %s", filepath);
                write(sock, "ERR_NM_DELETE\n", 14);
            }
        }
        
        // --- NM_CHECK_LOCKS ---
        else if (strcmp(command, "NM_CHECK_LOCKS") == 0) {
            // Check if file has any active locks
            // Use filepath (with SS_DATA_DIR prefix) to match how locks are stored
            if (is_file_locked(filepath)) {
                write(sock, "FILE_LOCKED\n", 12);
                log_message(SS_LOG_FILE, "INFO", "File %s has active locks", filepath);
            } else {
                write(sock, "FILE_UNLOCKED\n", 14);
                log_message(SS_LOG_FILE, "INFO", "File %s has no active locks", filepath);
            }
        }
        
        // --- NM_GETSIZE ---
        else if (strcmp(command, "NM_GETSIZE") == 0) {
            // Get the actual file size using stat
            struct stat st;
            if (stat(filepath, &st) == 0) {
                char size_response[BUFFER_SIZE];
                snprintf(size_response, sizeof(size_response), "SIZE %ld\n", st.st_size);
                write(sock, size_response, strlen(size_response));
                log_message(SS_LOG_FILE, "RESPONSE", "File %s size: %ld bytes", filepath, st.st_size);
            } else {
                write(sock, "SIZE 0\n", 7);
                log_message(SS_LOG_FILE, "WARNING", "Could not stat file %s", filepath);
            }
        }
        
        // --- NM_GETSTATS ---
        else if (strcmp(command, "NM_GETSTATS") == 0) {
            // Get detailed file statistics: size, word count, char count, last access time
            struct stat st;
            if (stat(filepath, &st) == 0) {
                // Calculate word count and character count
                FILE *fp = fopen(filepath, "r");
                long word_count = 0;
                long char_count = st.st_size; // Character count is file size
                
                if (fp) {
                    int in_word = 0;
                    int c;
                    while ((c = fgetc(fp)) != EOF) {
                        if (c == ' ' || c == '\n' || c == '\t') {
                            in_word = 0;
                        } else if (!in_word) {
                            in_word = 1;
                            word_count++;
                        }
                    }
                    fclose(fp);
                }
                
                char stats_response[BUFFER_SIZE];
                snprintf(stats_response, sizeof(stats_response), "STATS %ld %ld %ld %ld\n", 
                         st.st_size, word_count, char_count, st.st_atime);
                write(sock, stats_response, strlen(stats_response));
                log_message(SS_LOG_FILE, "RESPONSE", "File %s stats: size=%ld words=%ld chars=%ld", 
                           filepath, st.st_size, word_count, char_count);
            } else {
                write(sock, "STATS 0 0 0 0\n", 14);
                log_message(SS_LOG_FILE, "WARNING", "Could not stat file %s", filepath);
            }
        }
        
        // --- NM_CREATEFOLDER ---
        else if (strcmp(command, "NM_CREATEFOLDER") == 0) {
            // Create a folder using mkdir
            if (mkdir(filepath, 0755) == 0) {
                write(sock, "ACK_NM_CREATEFOLDER\n", 20);
                log_message(SS_LOG_FILE, "SUCCESS", "Created folder: %s", filepath);
            } else {
                perror("ERROR creating folder");
                log_message(SS_LOG_FILE, "ERROR", "Failed to create folder: %s", filepath);
                write(sock, "ERR_NM_CREATEFOLDER\n", 20);
            }
        }
        
        // --- NM_MOVE ---
        else if (strcmp(command, "NM_MOVE") == 0) {
            // arg2 contains the destination path (folder name or ".")
            char srcpath[BUFFER_SIZE];
            snprintf(srcpath, sizeof(srcpath), "%s/%s", SS_DATA_DIR, filename);

            char destpath[BUFFER_SIZE];
            if (strcmp(arg2, ".") == 0) {
                // Moving to root
                char* base_filename = get_base_filename_ss(filename);
                snprintf(destpath, sizeof(destpath), "%s/%s", SS_DATA_DIR, base_filename);
            } else {
                // Moving to a folder
                snprintf(destpath, sizeof(destpath), "%s/%s/%s", SS_DATA_DIR, arg2, get_base_filename_ss(filename));
            }
            
            if (rename(srcpath, destpath) == 0) {
                write(sock, "ACK_NM_MOVE\n", 12);
                printf("[SS-NMPort] Moved file %s to %s\n", srcpath, destpath);
            } else {
                perror("ERROR moving file");
                write(sock, "ERR_NM_MOVE\n", 12);
            }
        }
        
        // --- NM_WRITECONTENT ---
        else if (strcmp(command, "NM_WRITECONTENT") == 0) {
            int content_len;
            sscanf(buffer, "NM_WRITECONTENT %s %d", filename, &content_len);
            
            printf("[SS-NMPort] NM_WRITECONTENT: file=%s, expected_len=%d, read_size=%d\n", filename, content_len, read_size);
            
            // Calculate where the content starts in the buffer
            // The command line ends with \n, so find it
            char* newline_pos = strchr(buffer, '\n');
            int header_len = 0;
            int content_in_buffer = 0;
            
            if (newline_pos != NULL) {
                header_len = (newline_pos - buffer) + 1; // +1 to include the \n
                content_in_buffer = read_size - header_len;
                printf("[SS-NMPort] Header length: %d, content already in buffer: %d\n", header_len, content_in_buffer);
            }
            
            // Prepare buffer for content
            char content[8192];
            int total_read = 0;
            
            // First, copy any content that was already read in the initial buffer
            if (content_in_buffer > 0) {
                int to_copy = (content_in_buffer < content_len) ? content_in_buffer : content_len;
                memcpy(content, buffer + header_len, to_copy);
                total_read = to_copy;
                printf("[SS-NMPort] Copied %d bytes from initial buffer\n", to_copy);
            }
            
            // Keep reading until we get all the content
            while (total_read < content_len) {
                int bytes_read = read(sock, content + total_read, content_len - total_read);
                if (bytes_read <= 0) {
                    printf("[SS-NMPort] ERROR: Failed to read content (got %d of %d bytes, errno=%d)\n", total_read, content_len, errno);
                    write(sock, "ERR_NM_WRITECONTENT\n", 20);
                    break;
                }
                total_read += bytes_read;
                printf("[SS-NMPort] Read %d bytes (total: %d of %d)\n", bytes_read, total_read, content_len);
            }
            
            if (total_read == content_len) {
                // Write content to file
                FILE* f = fopen(filepath, "w");
                if (f != NULL) {
                    fwrite(content, 1, total_read, f);
                    fclose(f);
                    printf("[SS-NMPort] Wrote %d bytes to %s, sending ACK...\n", total_read, filepath);
                    int ack_sent = write(sock, "ACK_NM_WRITECONTENT\n", 20);
                    printf("[SS-NMPort] ACK sent (%d bytes)\n", ack_sent);
                } else {
                    perror("[SS-NMPort] ERROR opening file for writing");
                    write(sock, "ERR_NM_WRITECONTENT\n", 20);
                }
            }
        }
    }

    close(sock);
    printf("[SS-NMPort] NM connection closed.\n"); 
    return 0;
}

// --- Listener Thread for Name Server ---
void *start_nm_listener(void *port_arg) {
    int nm_port = *(int*)port_arg;
    free(port_arg);

    int listen_fd = create_server_socket(nm_port);
    log_message(SS_LOG_FILE, "SUCCESS", "Listening for NM connections on port %d", nm_port);

    while (1) {
        int conn_fd = accept(listen_fd, NULL, NULL);
        if (conn_fd < 0) {
            perror("ERROR on NM accept");
            continue;
        }
        
        char nm_ip[INET_ADDRSTRLEN];
        int nm_port_peer;
        get_client_info(conn_fd, nm_ip, &nm_port_peer);
        log_message(SS_LOG_FILE, "INFO", "NM connection from %s:%d", nm_ip, nm_port_peer);
        
        pthread_t thread_id;
        int *new_sock = malloc(sizeof(int));
        *new_sock = conn_fd;
        if (pthread_create(&thread_id, NULL, handle_nm_command, (void*) new_sock) < 0) {
            perror("ERROR creating NM handler thread");
            free(new_sock);
        }
        pthread_detach(thread_id);
    }
    close(listen_fd);
    return 0;
}

// --- Heartbeat Sender Thread ---
void *send_heartbeat(void *arg) {
    char* ss_id = (char*)arg;
    
    printf("[SS] Heartbeat thread started for SS %s\n", ss_id);
    
    while (1) {
        sleep(HEARTBEAT_INTERVAL);
        
        // Connect to NM's heartbeat port
        int hb_sock = connect_to_server(NM_IP, NM_HEARTBEAT_PORT);
        if (hb_sock < 0) {
            printf("[SS] WARNING: Failed to send heartbeat to NM\n");
            continue;
        }
        
        // Send heartbeat message: "HEARTBEAT <ss_id>\n"
        char hb_msg[BUFFER_SIZE];
        snprintf(hb_msg, sizeof(hb_msg), "HEARTBEAT %s\n", ss_id);
        write(hb_sock, hb_msg, strlen(hb_msg));
        close(hb_sock);
        
        printf("[SS] Heartbeat sent for SS %s\n", ss_id);
    }
    
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <ss_id> <client_port> <nm_port>\n", argv[0]);
        exit(1);
    }

    char* ss_id = argv[1];
    int client_port = atoi(argv[2]);
    int nm_port = atoi(argv[3]);

    // Store SS ID globally
    strncpy(SS_ID, ss_id, sizeof(SS_ID) - 1);
    SS_ID[sizeof(SS_ID) - 1] = '\0';

    // Set up log file path
    snprintf(SS_LOG_FILE, sizeof(SS_LOG_FILE), "logs/storage_server_%s.log", ss_id);
    init_log_file(SS_LOG_FILE);
    log_message(SS_LOG_FILE, "INFO", "=== Storage Server %s Starting ===", ss_id);

    // Create a dedicated data directory for this SS
    snprintf(SS_DATA_DIR, sizeof(SS_DATA_DIR), "ss_%s_data", ss_id);
    if (mkdir(SS_DATA_DIR, 0755) == -1 && errno != EEXIST) {
        die("ERROR creating data directory");
    }
    log_message(SS_LOG_FILE, "INFO", "Using data directory: %s", SS_DATA_DIR);

    // --- Step 1: Register with Name Server ---
    log_message(SS_LOG_FILE, "INFO", "Registering with Name Server at %s:%d", NM_IP, NM_PORT);
    int nm_sock = connect_to_server(NM_IP, NM_PORT);
    
    char reg_msg[BUFFER_SIZE];
    // Protocol: "REG_SS <id> <client_port> <nm_port>\n"
    // Name Server will auto-detect our IP from the socket connection
    snprintf(reg_msg, sizeof(reg_msg), "REG_SS %s %d %d\n", ss_id, client_port, nm_port);
    
    if (write(nm_sock, reg_msg, strlen(reg_msg)) < 0) die("ERROR writing to NM");
    
    char buffer[BUFFER_SIZE];
    // if (read(nm_sock, buffer, BUFFER_SIZE) < 0) die("ERROR reading from NM");
    ssize_t read_size = read(nm_sock, buffer, BUFFER_SIZE - 1);
    if (read_size < 0) {
        die("ERROR reading from NM");
    }
    buffer[read_size] = '\0'; 
    
    log_message(SS_LOG_FILE, "RESPONSE", "Name Server responded: %s", buffer);
    close(nm_sock);
    // printf("rimo\n");
    if (strncmp(buffer, "ACK_REG", 7) != 0) {
        die("ERROR: Name Server registration failed");
    }
    log_message(SS_LOG_FILE, "SUCCESS", "Registration successful");


    // --- Step 2: Start Heartbeat Thread ---
    pthread_t heartbeat_tid;
    char* ss_id_copy = strdup(ss_id);  // Duplicate for thread safety
    if (pthread_create(&heartbeat_tid, NULL, send_heartbeat, (void*)ss_id_copy) < 0) {
        die("ERROR creating heartbeat thread");
    }
    pthread_detach(heartbeat_tid);
    log_message(SS_LOG_FILE, "SUCCESS", "Heartbeat thread started");

    // --- Step 3: Start BOTH listener threads ---
    
    // We need to pass heap-allocated args to the threads
    int *client_port_ptr = malloc(sizeof(int));
    *client_port_ptr = client_port;
    int *nm_port_ptr = malloc(sizeof(int));
    *nm_port_ptr = nm_port;
    
    pthread_t client_tid, nm_tid;
    
    // Start Client Listener Thread
    if (pthread_create(&client_tid, NULL, start_client_listener, (void*) client_port_ptr) < 0) {
        die("ERROR creating client listener thread");
    }
    
    // Start NM Listener Thread
    if (pthread_create(&nm_tid, NULL, start_nm_listener, (void*) nm_port_ptr) < 0) {
        die("ERROR creating NM listener thread");
    }

    // Keep the main thread alive by joining the listeners
    pthread_join(client_tid, NULL);
    pthread_join(nm_tid, NULL); 

    return 0;
}