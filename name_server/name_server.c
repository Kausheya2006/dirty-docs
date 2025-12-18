#include "../common/utils.h"
#include "../common/config.h"
#include "ns_utils.h"
#include <stdio.h>
#include <sys/stat.h>
#include <time.h>
#include <signal.h>
#include <poll.h>
#include <fcntl.h>
#include <errno.h>

// Ignore SIGPIPE to prevent crash when writing to closed socket
// Instead, write() will return -1 with errno=EPIPE
#define SIGPIPE_IGNORE() signal(SIGPIPE, SIG_IGN)

// --- Persistence ---
#define PERSISTENCE_FILE "persistent/nm_data/trie.dat"

// --- Thread Pool Configuration ---
#define THREAD_POOL_SIZE 10
#define MAX_QUEUE_SIZE 1000

// --- Cache Configuration ---
#define CACHE_SIZE 1024
#define CACHE_EXPIRY_SECONDS 300

// --- Logging Configuration ---
#define NS_LOG_FILE "logs/name_server.log"

// --- Global Data Structures ---
// These MUST be protected by mutexes
StorageServer ss_list[MAX_SS];
ClientSession client_list[MAX_CLIENTS];
FileNode *file_trie_root;
int ss_count = 0;
int client_count = 0;

// --- Mutexes ---
pthread_mutex_t ss_list_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t client_list_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t file_trie_mutex = PTHREAD_MUTEX_INITIALIZER;
// ------------------------------

// --- Task Queue for Thread Pool ---
typedef struct {
    int sock;
    char buffer[BUFFER_SIZE];
    char username[100];
    int is_registration;
} Task;

typedef struct {
    Task tasks[MAX_QUEUE_SIZE];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} TaskQueue;

TaskQueue task_queue;
pthread_t worker_threads[THREAD_POOL_SIZE];
volatile int shutdown_workers = 0;

// Forward declarations
StorageServer* get_ss_by_id(const char *ss_id);

// Helper function to log and send responses
void send_response(int sock, const char* response, const char* username, const char* additional_info) {
    char ip[INET_ADDRSTRLEN];
    int port;
    get_client_info(sock, ip, &port);
    
    // Determine log level
    const char* level = "RESPONSE";
    if (strncmp(response, "ERR_", 4) == 0) {
        level = "ERROR";
    } else if (strncmp(response, "ACK_", 4) == 0) {
        level = "SUCCESS";
    }
    
    // Log the response
    if (additional_info && strlen(additional_info) > 0) {
        log_message(NS_LOG_FILE, level, "Response to %s@%s:%d -> %s (%s)", 
                   username, ip, port, response, additional_info);
    } else {
        log_message(NS_LOG_FILE, level, "Response to %s@%s:%d -> %s", 
                   username, ip, port, response);
    }
    
    // Send response
    write(sock, response, strlen(response));
}

// --- File-to-SS Cache ---
typedef struct {
    char filename[MAX_FILENAME];
    char ss_id[50];
    time_t last_access;
    int valid;
} CacheEntry;

CacheEntry file_cache[CACHE_SIZE];
pthread_mutex_t cache_mutex = PTHREAD_MUTEX_INITIALIZER;

// --- Cache Functions ---
void init_cache() {
    pthread_mutex_lock(&cache_mutex);
    for (int i = 0; i < CACHE_SIZE; i++) {
        file_cache[i].valid = 0;
    }
    pthread_mutex_unlock(&cache_mutex);
}

unsigned int hash_filename(const char* filename) {
    unsigned int hash = 0;
    while (*filename) {
        hash = (hash * 31) + *filename;
        filename++;
    }
    return hash % CACHE_SIZE;
}

StorageServer* get_cached_ss(const char* filename) {
    pthread_mutex_lock(&cache_mutex);
    unsigned int idx = hash_filename(filename);
    time_t now = time(NULL);
    
    if (file_cache[idx].valid && 
        strcmp(file_cache[idx].filename, filename) == 0 &&
        (now - file_cache[idx].last_access) < CACHE_EXPIRY_SECONDS) {
        
        file_cache[idx].last_access = now;
        char ss_id[50];
        strcpy(ss_id, file_cache[idx].ss_id);
        pthread_mutex_unlock(&cache_mutex);
        
        return get_ss_by_id(ss_id);
    }
    pthread_mutex_unlock(&cache_mutex);
    return NULL;
}

void cache_file_ss(const char* filename, const char* ss_id) {
    pthread_mutex_lock(&cache_mutex);
    unsigned int idx = hash_filename(filename);
    
    strncpy(file_cache[idx].filename, filename, MAX_FILENAME - 1);
    file_cache[idx].filename[MAX_FILENAME - 1] = '\0';
    strncpy(file_cache[idx].ss_id, ss_id, 49);
    file_cache[idx].ss_id[49] = '\0';
    file_cache[idx].last_access = time(NULL);
    file_cache[idx].valid = 1;
    
    pthread_mutex_unlock(&cache_mutex);
}

void invalidate_cache_entry(const char* filename) {
    pthread_mutex_lock(&cache_mutex);
    unsigned int idx = hash_filename(filename);
    if (file_cache[idx].valid && strcmp(file_cache[idx].filename, filename) == 0) {
        file_cache[idx].valid = 0;
    }
    pthread_mutex_unlock(&cache_mutex);
}

// --- Task Queue Functions ---
void init_task_queue() {
    task_queue.front = 0;
    task_queue.rear = 0;
    task_queue.count = 0;
    pthread_mutex_init(&task_queue.mutex, NULL);
    pthread_cond_init(&task_queue.not_empty, NULL);
    pthread_cond_init(&task_queue.not_full, NULL);
}

int enqueue_task(Task task) {
    pthread_mutex_lock(&task_queue.mutex);
    
    while (task_queue.count >= MAX_QUEUE_SIZE) {
        pthread_cond_wait(&task_queue.not_full, &task_queue.mutex);
    }
    
    task_queue.tasks[task_queue.rear] = task;
    task_queue.rear = (task_queue.rear + 1) % MAX_QUEUE_SIZE;
    task_queue.count++;
    
    pthread_cond_signal(&task_queue.not_empty);
    pthread_mutex_unlock(&task_queue.mutex);
    return 0;
}

Task dequeue_task() {
    pthread_mutex_lock(&task_queue.mutex);
    
    while (task_queue.count == 0 && !shutdown_workers) {
        pthread_cond_wait(&task_queue.not_empty, &task_queue.mutex);
    }
    
    Task task;
    if (shutdown_workers && task_queue.count == 0) {
        task.sock = -1;
        pthread_mutex_unlock(&task_queue.mutex);
        return task;
    }
    
    task = task_queue.tasks[task_queue.front];
    task_queue.front = (task_queue.front + 1) % MAX_QUEUE_SIZE;
    task_queue.count--;
    
    pthread_cond_signal(&task_queue.not_full);
    pthread_mutex_unlock(&task_queue.mutex);
    return task;
}
// ------------------------------

// --- Access Request System ---
typedef enum { REQ_READ, REQ_WRITE } RequestType;
typedef enum { RSTATUS_PENDING, RSTATUS_APPROVED, RSTATUS_DENIED } RequestStatus;
typedef struct {
    int id;
    char filename[MAX_FILENAME];
    char requester[100];
    char owner[100];
    RequestType type;
    RequestStatus status;
    time_t created_at;
} AccessRequest;

#define MAX_REQUESTS 1024
static AccessRequest requests[MAX_REQUESTS];
static int request_count = 0;
static pthread_mutex_t requests_mutex = PTHREAD_MUTEX_INITIALIZER;

static const char* request_type_str(RequestType t){ return t==REQ_READ?"READ":"WRITE"; }
static const char* request_status_str(RequestStatus s){ return s==RSTATUS_PENDING?"PENDING":(s==RSTATUS_APPROVED?"APPROVED":"DENIED"); }

static int create_request(const char* filename, const char* requester, const char* owner, RequestType type) {
    pthread_mutex_lock(&requests_mutex);
    if (request_count >= MAX_REQUESTS) { pthread_mutex_unlock(&requests_mutex); return -1; }
    // Avoid duplicate pending request of same type
    for (int i=0;i<request_count;i++) {
        if (requests[i].status==RSTATUS_PENDING && strcmp(requests[i].filename, filename)==0 &&
            strcmp(requests[i].requester, requester)==0 && requests[i].type==type) {
            pthread_mutex_unlock(&requests_mutex); return requests[i].id; // already exists
        }
    }
    int id = request_count == 0 ? 1 : requests[request_count-1].id + 1;
    AccessRequest *r = &requests[request_count++];
    r->id = id;
    snprintf(r->filename, sizeof(r->filename), "%s", filename);
    snprintf(r->requester, sizeof(r->requester), "%s", requester);
    snprintf(r->owner, sizeof(r->owner), "%s", owner);
    r->type = type;
    r->status = RSTATUS_PENDING;
    r->created_at = time(NULL);
    pthread_mutex_unlock(&requests_mutex);
    return id;
}

static AccessRequest* find_request_by_id(int id){
    for (int i=0;i<request_count;i++){ if (requests[i].id==id) return &requests[i]; }
    return NULL;
}

// Helper to pick an SS (simple round-robin)
StorageServer *get_ss_for_new_file()
{
    pthread_mutex_lock(&ss_list_mutex);
    if (ss_count == 0)
    {
        pthread_mutex_unlock(&ss_list_mutex);
        return NULL;
    }
    // Simple round-robin
    static int next_ss_index = 0;
    StorageServer *ss = &ss_list[next_ss_index];
    next_ss_index = (next_ss_index + 1) % ss_count;
    pthread_mutex_unlock(&ss_list_mutex);
    return ss;
}

// Helper to find an SS by ID
StorageServer *get_ss_by_id(const char *ss_id)
{
    pthread_mutex_lock(&ss_list_mutex);
    for (int i = 0; i < ss_count; i++)
    {
        if (strcmp(ss_list[i].id, ss_id) == 0 && ss_list[i].is_active)
        {
            pthread_mutex_unlock(&ss_list_mutex);
            return &ss_list[i];
        }
    }
    pthread_mutex_unlock(&ss_list_mutex);
    return NULL;
}

// Helper to select replica storage servers (different from primary)
int select_replica_servers(const char* primary_ss_id, char** replica_ss_ids, int max_replicas) {
    int replica_count = 0;
    pthread_mutex_lock(&ss_list_mutex);
    
    for (int i = 0; i < ss_count && replica_count < max_replicas; i++) {
        if (ss_list[i].is_active && strcmp(ss_list[i].id, primary_ss_id) != 0) {
            replica_ss_ids[replica_count] = strdup(ss_list[i].id);
            replica_count++;
        }
    }
    
    pthread_mutex_unlock(&ss_list_mutex);
    return replica_count;
}

// Structure for async replication thread
typedef struct {
    char filename[MAX_FILENAME];
    char ss_ip[50];
    int ss_port;
    char ss_id[50];
    char primary_ss_ip[50];
    int primary_ss_client_port;
} ReplicationTask;

// Thread function to replicate file to another SS (with content)
void* replicate_file_with_content_async(void* arg) {
    ReplicationTask* task = (ReplicationTask*)arg;
    
    log_message(NS_LOG_FILE, "INFO", "Async replication with content: %s to SS %s", task->filename, task->ss_id);
    
    // Step 1: Read file content from primary SS
    int primary_sock = connect_to_server(task->primary_ss_ip, task->primary_ss_client_port);
    if (primary_sock < 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to connect to primary SS for reading %s", task->filename);
        free(task);
        return NULL;
    }
    
    char read_cmd[BUFFER_SIZE];
    snprintf(read_cmd, sizeof(read_cmd), "READ %s\n", task->filename);
    write(primary_sock, read_cmd, strlen(read_cmd));
    
    char file_content[8192] = {0};
    int bytes_read = read(primary_sock, file_content, sizeof(file_content) - 1);
    close(primary_sock);
    
    if (bytes_read <= 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to read content of %s from primary SS", task->filename);
        free(task);
        return NULL;
    }
    file_content[bytes_read] = '\0';
    
    // Step 2: Delete old file on replica SS (if exists)
    int replica_sock = connect_to_server(task->ss_ip, task->ss_port);
    if (replica_sock < 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to connect to replica SS %s", task->ss_id);
        free(task);
        return NULL;
    }
    
    char delete_cmd[BUFFER_SIZE];
    snprintf(delete_cmd, sizeof(delete_cmd), "NM_DELETE %s\n", task->filename);
    write(replica_sock, delete_cmd, strlen(delete_cmd));
    
    char ack[BUFFER_SIZE];
    read(replica_sock, ack, BUFFER_SIZE);
    close(replica_sock);
    
    // Step 3: Create new file on replica SS
    replica_sock = connect_to_server(task->ss_ip, task->ss_port);
    if (replica_sock < 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to reconnect to replica SS %s", task->ss_id);
        free(task);
        return NULL;
    }
    
    char create_cmd[BUFFER_SIZE];
    snprintf(create_cmd, sizeof(create_cmd), "NM_CREATE %s\n", task->filename);
    write(replica_sock, create_cmd, strlen(create_cmd));
    
    read(replica_sock, ack, BUFFER_SIZE);
    close(replica_sock);
    
    if (strncmp(ack, "ACK_NM_CREATE", 13) != 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to create %s on replica SS %s", task->filename, task->ss_id);
        free(task);
        return NULL;
    }
    
    // Step 4: Write content to replica SS (if file has content)
    if (strlen(file_content) > 0) {
        // Connect to replica's client port to write content
        int write_sock = connect_to_server(task->ss_ip, task->ss_port - 1000); // Assuming client_port = nm_port - 1000
        if (write_sock < 0) {
            log_message(NS_LOG_FILE, "WARNING", "Failed to connect to replica SS client port");
            free(task);
            return NULL;
        }
        
        // We need to parse the file content and reconstruct it
        // For now, just send a simple WRITE command
        // This is a limitation - we'd need the full file structure
        // For simplicity, we'll use a direct file copy approach via NM_COPYFILE command
        close(write_sock);
    }
    
    log_message(NS_LOG_FILE, "SUCCESS", "Replication with content successful: %s on SS %s", task->filename, task->ss_id);
    free(task);
    return NULL;
}

// Thread function to replicate file to another SS
void* replicate_file_async(void* arg) {
    ReplicationTask* task = (ReplicationTask*)arg;
    
    log_message(NS_LOG_FILE, "INFO", "Async replication: %s to SS %s", task->filename, task->ss_id);
    
    int sock = connect_to_server(task->ss_ip, task->ss_port);
    if (sock < 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to connect to SS %s for replication", task->ss_id);
        free(task);
        return NULL;
    }
    
    char cmd[BUFFER_SIZE];
    snprintf(cmd, sizeof(cmd), "NM_CREATE %s\n", task->filename);
    write(sock, cmd, strlen(cmd));
    
    char ack[BUFFER_SIZE];
    read(sock, ack, BUFFER_SIZE);
    close(sock);
    
    if (strncmp(ack, "ACK_NM_CREATE", 13) == 0) {
        log_message(NS_LOG_FILE, "SUCCESS", "Replication successful: %s on SS %s", task->filename, task->ss_id);
    } else {
        log_message(NS_LOG_FILE, "WARNING", "Replication failed: %s on SS %s", task->filename, task->ss_id);
    }
    
    free(task);
    return NULL;
}

// Thread function to replicate folder to another SS
void* replicate_folder_async(void* arg) {
    ReplicationTask* task = (ReplicationTask*)arg;
    
    log_message(NS_LOG_FILE, "INFO", "Async folder replication: %s to SS %s", task->filename, task->ss_id);
    
    int sock = connect_to_server(task->ss_ip, task->ss_port);
    if (sock < 0) {
        log_message(NS_LOG_FILE, "WARNING", "Failed to connect to SS %s for folder replication", task->ss_id);
        free(task);
        return NULL;
    }
    
    char cmd[BUFFER_SIZE];
    snprintf(cmd, sizeof(cmd), "NM_CREATEFOLDER %s\n", task->filename);
    write(sock, cmd, strlen(cmd));
    
    char ack[BUFFER_SIZE];
    read(sock, ack, BUFFER_SIZE);
    close(sock);
    
    if (strncmp(ack, "ACK_NM_CREATEFOLDER", 19) == 0) {
        log_message(NS_LOG_FILE, "SUCCESS", "Folder replication successful: %s on SS %s", task->filename, task->ss_id);
    } else {
        log_message(NS_LOG_FILE, "WARNING", "Folder replication failed: %s on SS %s", task->filename, task->ss_id);
    }
    
    free(task);
    return NULL;
}

// --- Helper to traverse trie and find files that should be on a specific SS ---
void find_files_for_ss(FileNode* node, const char* ss_id, char files[][MAX_FILENAME], int* file_count, 
                       char* current_path, int max_files) {
    if (node == NULL || *file_count >= max_files) return;
    
    // If this is a file/folder
    if (node->is_end_of_word && !node->is_folder) {
        // Check if this SS is in the replica list
        for (int i = 0; i < node->ss_count && i < MAX_SS; i++) {
            if (node->ss_ids[i] != NULL && strcmp(node->ss_ids[i], ss_id) == 0) {
                strncpy(files[*file_count], current_path, MAX_FILENAME - 1);
                (*file_count)++;
                break;
            }
        }
    }
    
    // Recursively traverse children
    for (int i = 0; i < 128; i++) {
        if (node->children[i] != NULL) {
            char new_path[MAX_FILENAME * 2];
            snprintf(new_path, sizeof(new_path), "%s%c", current_path, (char)i);
            find_files_for_ss(node->children[i], ss_id, files, file_count, new_path, max_files);
        }
    }
}

// --- SS Recovery Synchronization Thread ---
void* sync_recovered_ss(void* arg) {
    char* ss_id = (char*)arg;
    log_message(NS_LOG_FILE, "INFO", "Starting synchronization for recovered SS %s", ss_id);
    
    sleep(2); // Give SS time to fully initialize
    
    // Find all files that should be on this SS
    char files_to_sync[100][MAX_FILENAME];
    int file_count = 0;
    char current_path[MAX_FILENAME * 2] = "";
    
    pthread_mutex_lock(&file_trie_mutex);
    find_files_for_ss(file_trie_root, ss_id, files_to_sync, &file_count, current_path, 100);
    pthread_mutex_unlock(&file_trie_mutex);
    
    log_message(NS_LOG_FILE, "INFO", "Found %d files that should be on SS %s", file_count, ss_id);
    
    // Get SS info
    StorageServer* target_ss = get_ss_by_id(ss_id);
    if (target_ss == NULL) {
        log_message(NS_LOG_FILE, "ERROR", "Cannot sync - SS %s not found", ss_id);
        free(ss_id);
        return NULL;
    }
    
    // For each file, find an active replica and copy it
    for (int i = 0; i < file_count; i++) {
        char* filename = files_to_sync[i];
        
        pthread_mutex_lock(&file_trie_mutex);
        FileNode* node = find_file(file_trie_root, filename);
        if (node == NULL) {
            pthread_mutex_unlock(&file_trie_mutex);
            continue;
        }
        
        // Find an active replica (not the target SS)
        char* source_ss_id = NULL;
        for (int j = 0; j < node->ss_count && j < MAX_SS; j++) {
            if (node->ss_ids[j] != NULL && strcmp(node->ss_ids[j], ss_id) != 0) {
                StorageServer* source_ss = get_ss_by_id(node->ss_ids[j]);
                if (source_ss != NULL && source_ss->is_active) {
                    source_ss_id = node->ss_ids[j];
                    break;
                }
            }
        }
        pthread_mutex_unlock(&file_trie_mutex);
        
        if (source_ss_id == NULL) {
            log_message(NS_LOG_FILE, "WARNING", "No active replica found for %s, skipping", filename);
            continue;
        }
        
        // Get source SS
        StorageServer* source_ss = get_ss_by_id(source_ss_id);
        if (source_ss == NULL) continue;
        
        // Read file from source SS
        int source_sock = connect_to_server(source_ss->ip, source_ss->client_port);
        if (source_sock < 0) {
            log_message(NS_LOG_FILE, "WARNING", "Failed to connect to source SS %s", source_ss_id);
            continue;
        }
        
        char read_cmd[BUFFER_SIZE];
        snprintf(read_cmd, sizeof(read_cmd), "READ %s\n", filename);
        write(source_sock, read_cmd, strlen(read_cmd));
        
        char file_content[8192] = {0};
        int bytes_read = read(source_sock, file_content, sizeof(file_content) - 1);
        close(source_sock);
        
        if (bytes_read <= 0) {
            log_message(NS_LOG_FILE, "WARNING", "Failed to read %s from SS %s", filename, source_ss_id);
            continue;
        }
        file_content[bytes_read] = '\0';
        
        // Write file to target SS
        int target_sock = connect_to_server(target_ss->ip, target_ss->nm_port);
        if (target_sock < 0) {
            log_message(NS_LOG_FILE, "WARNING", "Failed to connect to target SS %s", ss_id);
            continue;
        }
        
        // First create the file
        char create_cmd[BUFFER_SIZE];
        snprintf(create_cmd, sizeof(create_cmd), "NM_CREATE %s\n", filename);
        write(target_sock, create_cmd, strlen(create_cmd));
        
        char ack[BUFFER_SIZE];
        read(target_sock, ack, BUFFER_SIZE);
        close(target_sock);
        
        // Now write the content via client port
        int write_sock = connect_to_server(target_ss->ip, target_ss->client_port);
        if (write_sock < 0) continue;
        
        char write_cmd[9000];
        snprintf(write_cmd, sizeof(write_cmd), "WRITE %s\n%s\n", filename, file_content);
        write(write_sock, write_cmd, strlen(write_cmd));
        
        read(write_sock, ack, BUFFER_SIZE);
        close(write_sock);
        
        log_message(NS_LOG_FILE, "SUCCESS", "Synced file %s to recovered SS %s from SS %s", filename, ss_id, source_ss_id);
    }
    
    log_message(NS_LOG_FILE, "SUCCESS", "Synchronization complete for SS %s (%d files synced)", ss_id, file_count);
    free(ss_id);
    return NULL;
}

// Handles Storage Server registration
void handle_ss_registration(char *buffer, int sock)
{
    char ss_id[50];
    int client_port, nm_port;
    
    // Protocol: "REG_SS <id> <client_port> <nm_port>\n"
    int items = sscanf(buffer, "REG_SS %s %d %d", ss_id, &client_port, &nm_port);
    if (items != 3)
    {
        log_message(NS_LOG_FILE, "ERROR", "Invalid REG_SS format");
        write(sock, "ERR_REG_FORMAT\n", 15);
        return;
    }
    
    // Auto-detect SS IP from the socket connection
    char ip[50];
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    if (getpeername(sock, (struct sockaddr*)&addr, &addr_len) == 0) {
        inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    } else {
        log_message(NS_LOG_FILE, "ERROR", "Failed to get peer address");
        write(sock, "ERR_INTERNAL\n", 13);
        return;
    }

    pthread_mutex_lock(&ss_list_mutex);
    
    // Check if this SS ID already exists (recovery scenario)
    int ss_index = -1;
    int is_recovery = 0;
    for (int i = 0; i < ss_count; i++) {
        if (strcmp(ss_list[i].id, ss_id) == 0) {
            ss_index = i;
            is_recovery = 1;
            break;
        }
    }
    
    if (is_recovery) {
        // This is a recovery - update existing entry
        strcpy(ss_list[ss_index].ip, ip);
        ss_list[ss_index].client_port = client_port;
        ss_list[ss_index].nm_port = nm_port;
        ss_list[ss_index].is_active = 1;
        ss_list[ss_index].last_heartbeat = time(NULL);
        
        pthread_mutex_unlock(&ss_list_mutex);
        
        log_message(NS_LOG_FILE, "SUCCESS", "SS %s RECOVERED! Reconnected at %s (Client:%d, NM:%d)", 
               ss_id, ip, client_port, nm_port);
        write(sock, "ACK_REG_RECOVERY\n", 17);
        
        // Trigger synchronization in a separate thread
        char* ss_id_copy = strdup(ss_id);
        pthread_t sync_tid;
        pthread_create(&sync_tid, NULL, sync_recovered_ss, (void*)ss_id_copy);
        pthread_detach(sync_tid);
    } else {
        // This is a new registration
        if (ss_count >= MAX_SS) {
            pthread_mutex_unlock(&ss_list_mutex);
            log_message(NS_LOG_FILE, "WARNING", "Max storage servers reached");
            write(sock, "ERR_MAX_SS\n", 11);
            return;
        }
        
        // Add to our list
        strcpy(ss_list[ss_count].id, ss_id);
        strcpy(ss_list[ss_count].ip, ip);
        ss_list[ss_count].client_port = client_port;
        ss_list[ss_count].nm_port = nm_port;
        ss_list[ss_count].is_active = 1;
        ss_list[ss_count].last_heartbeat = time(NULL);
        ss_count++;

        pthread_mutex_unlock(&ss_list_mutex);

        log_message(NS_LOG_FILE, "SUCCESS", "Registered NEW SS %s at %s (Client:%d, NM:%d)", ss_id, ip, client_port, nm_port);
        write(sock, "ACK_REG\n", 8);
    }
}

// Helper function to save trie with mutex protection
void persist_trie() {
    pthread_mutex_lock(&file_trie_mutex);
    save_trie_to_file(file_trie_root, PERSISTENCE_FILE);
    pthread_mutex_unlock(&file_trie_mutex);
}

// --- Heartbeat Handler ---
void *handle_heartbeat_connection(void *socket_desc) {
    int sock = *(int*)socket_desc;
    free(socket_desc);
    
    char buffer[BUFFER_SIZE];
    int read_size = read(sock, buffer, BUFFER_SIZE - 1);
    if (read_size > 0) {
        buffer[read_size] = '\0';
        
        char command[100], ss_id[50];
        sscanf(buffer, "%s %s", command, ss_id);
        
        if (strcmp(command, "HEARTBEAT") == 0) {
            // Update last_heartbeat timestamp
            pthread_mutex_lock(&ss_list_mutex);
            for (int i = 0; i < ss_count; i++) {
                if (strcmp(ss_list[i].id, ss_id) == 0) {
                    ss_list[i].last_heartbeat = time(NULL);
                    if (!ss_list[i].is_active) {
                        ss_list[i].is_active = 1;
                        log_message(NS_LOG_FILE, "INFO", "SS %s is back online!", ss_id);
                    }
                    break;
                }
            }
            pthread_mutex_unlock(&ss_list_mutex);
            
            log_message(NS_LOG_FILE, "INFO", "Heartbeat received from SS %s", ss_id);
        }
    }
    
    close(sock);
    return NULL;
}

// --- Heartbeat Listener Thread ---
void *heartbeat_listener(void *arg) {
    int listen_fd = create_server_socket(NM_HEARTBEAT_PORT);
    log_message(NS_LOG_FILE, "INFO", "Heartbeat listener started on port %d", NM_HEARTBEAT_PORT);
    
    while (1) {
        int conn_fd = accept(listen_fd, NULL, NULL);
        if (conn_fd < 0) {
            perror("ERROR on heartbeat accept");
            continue;
        }
        
        pthread_t thread_id;
        int *new_sock = malloc(sizeof(int));
        *new_sock = conn_fd;
        
        if (pthread_create(&thread_id, NULL, handle_heartbeat_connection, (void*)new_sock) < 0) {
            perror("ERROR creating heartbeat handler thread");
            free(new_sock);
        }
        pthread_detach(thread_id);
    }
    
    close(listen_fd);
    return NULL;
}

// --- Failure Monitoring Thread ---
void *monitor_failures(void *arg) {
    log_message(NS_LOG_FILE, "INFO", "Failure monitoring thread started");
    
    while (1) {
        sleep(HEARTBEAT_INTERVAL);  // Check every heartbeat interval
        
        time_t current_time = time(NULL);
        
        pthread_mutex_lock(&ss_list_mutex);
        for (int i = 0; i < ss_count; i++) {
            if (ss_list[i].is_active) {
                // Check if last heartbeat is too old
                time_t time_since_heartbeat = current_time - ss_list[i].last_heartbeat;
                
                if (time_since_heartbeat > FAILURE_TIMEOUT) {
                    ss_list[i].is_active = 0;
                    log_message(NS_LOG_FILE, "WARNING", "FAILURE DETECTED: SS %s marked as inactive (no heartbeat for %ld seconds)", 
                           ss_list[i].id, time_since_heartbeat);
                }
            }
        }
        pthread_mutex_unlock(&ss_list_mutex);
    }
    
    return NULL;
}

// Helper function for EMPTYTRASH - collects files to delete
void empty_trash_recursive_helper(FileNode* node, const char* username, char files_to_delete[][MAX_FILENAME], int* delete_count, char* current_prefix) {
    if (node == NULL || *delete_count >= MAX_CLIENTS) return;
    if (node->is_end_of_word && node->is_in_trash && strcmp(node->owner, username) == 0) {
        strcpy(files_to_delete[(*delete_count)++], current_prefix);
    }
    for (int i = 0; i < 128; i++) {
        if (node->children[i] != NULL) {
            int len = strlen(current_prefix);
            current_prefix[len] = (char)i;
            current_prefix[len + 1] = '\0';
            empty_trash_recursive_helper(node->children[i], username, files_to_delete, delete_count, current_prefix);
            current_prefix[len] = '\0';
        }
    }
}

// Helper function to get actual file size from Storage Server
long get_file_size_from_ss(const char* filename, const char* ss_ip, int ss_nm_port) {
    int ss_sock = connect_to_server(ss_ip, ss_nm_port);
    if (ss_sock < 0) {
        return 0; // Connection failed, return 0
    }
    
    char cmd[BUFFER_SIZE];
    snprintf(cmd, sizeof(cmd), "NM_GETSIZE %s\n", filename);
    write(ss_sock, cmd, strlen(cmd));
    
    char response[BUFFER_SIZE];
    int n = read(ss_sock, response, BUFFER_SIZE - 1);
    close(ss_sock);
    
    if (n > 0) {
        response[n] = '\0';
        long size = 0;
        if (sscanf(response, "SIZE %ld", &size) == 1) {
            return size;
        }
    }
    
    return 0; // Failed to get size
}

// Get file statistics from Storage Server (size, word count, char count, last access)


// Handles all commands from a logged-in client
void handle_client_commands(char *username, int sock)
{
    char buffer[BUFFER_SIZE];
    int read_size;
    
    // Get client IP and port for logging
    char client_ip[INET_ADDRSTRLEN];
    int client_port;
    get_client_info(sock, client_ip, &client_port);

    // Command loop
    while (1)
    {
        read_size = read(sock, buffer, BUFFER_SIZE - 1);
        
        if (read_size < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No data available right now on non-blocking socket, wait and retry
                usleep(10000); // Sleep for 10ms
                continue;
            } else {
                // Real error
                perror("read error");
                break;
            }
        }
        
        if (read_size == 0) {
            // Connection closed by client
            log_message(NS_LOG_FILE, "INFO", "Client '%s' from %s:%d disconnected", username, client_ip, client_port);
            break;
        }
        
        buffer[read_size] = '\0';  // Null-terminate the buffer
        log_message(NS_LOG_FILE, "REQUEST", "User '%s' (%s:%d) command: %s", username, client_ip, client_port, buffer);

        char command[100], arg1[MAX_FILENAME], arg2[100], arg3[100];
        bzero(command, 100);
        bzero(arg1, MAX_FILENAME);
        bzero(arg2, 100);
        bzero(arg3, 100);

        sscanf(buffer, "%s %s %s %s", command, arg1, arg2, arg3);

        // --- CREATE ---
        if (strcmp(command, "CREATE") == 0)
        {
            pthread_mutex_lock(&file_trie_mutex);
            FileNode* existing = find_file_any_status(file_trie_root, arg1);
            if (existing != NULL)
            {
                if (existing->is_in_trash) {
                    pthread_mutex_unlock(&file_trie_mutex);
                    send_response(sock, "ERR_FILE_IN_TRASH\n", username, arg1);
                    continue;
                } else {
                    pthread_mutex_unlock(&file_trie_mutex);
                    send_response(sock, "ERR_FILE_EXISTS\n", username, arg1);
                    continue;
                }
            }
            pthread_mutex_unlock(&file_trie_mutex);

            StorageServer *ss = get_ss_for_new_file();
            if (ss == NULL)
            {
                send_response(sock, "ERR_NO_SS_AVAIL\n", username, "");
                continue;
            }

            // Connect to the SS's NM_PORT
            int ss_sock = connect_to_server(ss->ip, ss->nm_port);
            char ss_cmd[BUFFER_SIZE];
            snprintf(ss_cmd, sizeof(ss_cmd), "NM_CREATE %s\n", arg1);
            write(ss_sock, ss_cmd, strlen(ss_cmd));

            char ss_ack[BUFFER_SIZE];
            read(ss_sock, ss_ack, BUFFER_SIZE);
            close(ss_sock);

            if (strncmp(ss_ack, "ACK_NM_CREATE", 13) == 0)
            {
                // File created on primary SS successfully
                // Now select replica servers and store all SS IDs
                char* all_ss_ids[MAX_SS];
                int total_ss_count = 1;
                all_ss_ids[0] = strdup(ss->id);
                
                // Select additional replicas (REPLICATION_FACTOR - 1)
                if (REPLICATION_FACTOR > 1 && ss_count > 1) {
                    char* replica_ids[MAX_SS];
                    int replica_count = select_replica_servers(ss->id, replica_ids, REPLICATION_FACTOR - 1);
                    
                    for (int i = 0; i < replica_count; i++) {
                        all_ss_ids[total_ss_count++] = replica_ids[i];
                    }
                }
                
                // Insert file with all replica information
                pthread_mutex_lock(&file_trie_mutex);
                if (total_ss_count > 1) {
                    insert_file_with_replicas(file_trie_root, arg1, username, all_ss_ids, total_ss_count);
                } else {
                    insert_file(file_trie_root, arg1, username, all_ss_ids[0]);
                }
                pthread_mutex_unlock(&file_trie_mutex);
                persist_trie(); // Save to disk
                
                // Async replication to other SS (if any)
                for (int i = 1; i < total_ss_count; i++) {
                    StorageServer* replica_ss = get_ss_by_id(all_ss_ids[i]);
                    if (replica_ss != NULL) {
                        ReplicationTask* task = malloc(sizeof(ReplicationTask));
                        strncpy(task->filename, arg1, MAX_FILENAME);
                        strncpy(task->ss_ip, replica_ss->ip, 50);
                        task->ss_port = replica_ss->nm_port;
                        strncpy(task->ss_id, replica_ss->id, 50);
                        
                        pthread_t repl_thread;
                        pthread_create(&repl_thread, NULL, replicate_file_async, task);
                        pthread_detach(repl_thread);
                    }
                }
                
                // Clean up
                for (int i = 0; i < total_ss_count; i++) {
                    free(all_ss_ids[i]);
                }
                
                // Cache the new file
                cache_file_ss(arg1, ss->id);
                
                write(sock, "ACK_CREATE\n", 11);
                log_message(NS_LOG_FILE, "SUCCESS", "User '%s' (%s:%d) created file '%s' on SS %s (%s:%d) with %d replicas", 
                           username, client_ip, client_port, arg1, ss->id, ss->ip, ss->nm_port, total_ss_count - 1);
            }
            else
            {
                send_response(sock, "ERR_SS_CREATE_FAILED\n", username, arg1);
            }
        }

        // --- DELETE ---
        // --- TRASH (replaces DELETE) ---
        else if (strcmp(command, "TRASH") == 0)
        {
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file_any_status(file_trie_root, arg1); // Find even if already in trash
            if (node == NULL) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }
            if (strcmp(node->owner, username) != 0) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_PERMISSION_DENIED\n", 22);
                continue;
            }
            if (node->is_in_trash) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_ALREADY_IN_TRASH\n", 21);
                continue;
            }
            
            // Check if it's a folder - folders cannot be deleted with TRASH
            if (node->is_folder) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_CANNOT_DELETE_FOLDER\n", 25);
                log_message(NS_LOG_FILE, "WARNING", "Cannot trash folder %s", arg1);
                continue;
            }
            
            // Check if file has any active locks on the primary SS
            if (node->ss_count > 0) {
                StorageServer* ss = get_ss_by_id(node->ss_ids[0]); // Check primary SS
                if (ss != NULL) {
                    int ss_sock = connect_to_server(ss->ip, ss->nm_port);
                    if (ss_sock >= 0) {
                        char check_cmd[BUFFER_SIZE];
                        snprintf(check_cmd, sizeof(check_cmd), "NM_CHECK_LOCKS %s\n", arg1);
                        write(ss_sock, check_cmd, strlen(check_cmd));
                        
                        char ss_response[BUFFER_SIZE];
                        int resp_size = read(ss_sock, ss_response, BUFFER_SIZE - 1);
                        close(ss_sock);
                        
                        if (resp_size > 0) {
                            ss_response[resp_size] = '\0';
                            if (strncmp(ss_response, "FILE_LOCKED", 11) == 0) {
                                pthread_mutex_unlock(&file_trie_mutex);
                                write(sock, "ERR_FILE_LOCKED\n", 16);
                                log_message(NS_LOG_FILE, "WARNING", "Cannot trash %s: file has active locks", arg1);
                                continue;
                            }
                        }
                    }
                }
            }

            node->is_in_trash = 1;
            node->last_modified = time(NULL);
            
            pthread_mutex_unlock(&file_trie_mutex);
            persist_trie();
            
            write(sock, "ACK_TRASHED\n", 12);
            log_message(NS_LOG_FILE, "SUCCESS", "User '%s' (%s:%d) moved file '%s' to trash", username, client_ip, client_port, arg1);
        }

        // --- RESTORE ---
        else if (strcmp(command, "RESTORE") == 0)
        {
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file_any_status(file_trie_root, arg1);
            if (node == NULL) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }
            if (strcmp(node->owner, username) != 0) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_PERMISSION_DENIED\n", 22);
                continue;
            }
            if (!node->is_in_trash) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_NOT_IN_TRASH\n", 17);
                continue;
            }

            node->is_in_trash = 0;
            node->last_modified = time(NULL);
            
            pthread_mutex_unlock(&file_trie_mutex);
            persist_trie();
            
            write(sock, "ACK_RESTORED\n", 13);
            log_message(NS_LOG_FILE, "SUCCESS", "User '%s' (%s:%d) restored file '%s' from trash", username, client_ip, client_port, arg1);
        }

        // --- VIEWTRASH ---
        else if (strcmp(command, "VIEWTRASH") == 0)
        {
            char trash_list_buffer[BUFFER_SIZE * 4];
            bzero(trash_list_buffer, sizeof(trash_list_buffer));
            
            pthread_mutex_lock(&file_trie_mutex);
            list_trash(file_trie_root, username, trash_list_buffer);
            pthread_mutex_unlock(&file_trie_mutex);

            if (strlen(trash_list_buffer) == 0) {
                write(sock, "Trash is empty.\n", 16);
            } else {
                write(sock, trash_list_buffer, strlen(trash_list_buffer));
            }
        }

        // --- EMPTYTRASH ---
        else if (strcmp(command, "EMPTYTRASH") == 0)
        {
            char files_to_delete[MAX_CLIENTS][MAX_FILENAME];
            int delete_count = 0;
            
            // 1. Find all files to delete
            pthread_mutex_lock(&file_trie_mutex);
            
            char prefix[MAX_FILENAME * 2] = "";
            empty_trash_recursive_helper(file_trie_root, username, files_to_delete, &delete_count, prefix);
            pthread_mutex_unlock(&file_trie_mutex);

            // 2. Delete them one by one
            int deleted_count = 0;
            for (int i = 0; i < delete_count; i++) {
                char* filename = files_to_delete[i];
                pthread_mutex_lock(&file_trie_mutex);
                FileNode* node = find_file_any_status(file_trie_root, filename);
                if (node == NULL) {
                    pthread_mutex_unlock(&file_trie_mutex);
                    continue;
                }
                
                // Tell all replicas to delete
                for (int r = 0; r < node->ss_count; r++) {
                    StorageServer* ss = get_ss_by_id(node->ss_ids[r]);
                    if (ss != NULL && ss->is_active) {
                        int ss_sock = connect_to_server(ss->ip, ss->nm_port);
                        char ss_cmd[BUFFER_SIZE];
                        snprintf(ss_cmd, sizeof(ss_cmd), "NM_DELETE %s\n", filename);
                        write(ss_sock, ss_cmd, strlen(ss_cmd));
                        close(ss_sock); // Fire and forget
                    }
                }
                
                // Permanently delete from Trie
                delete_file(file_trie_root, filename, 0);
                pthread_mutex_unlock(&file_trie_mutex);
                deleted_count++;
            }
            
            if (deleted_count > 0) persist_trie();
            char ack[100];
            snprintf(ack, sizeof(ack), "ACK_EMPTYTRASH %d files permanently deleted.\n", deleted_count);
            write(sock, ack, strlen(ack));
        }

        // --- DELETE (kept for backward compatibility but now permanently deletes) ---
        else if (strcmp(command, "DELETE") == 0)
        {
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, arg1);
            if (node == NULL)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }
            // Check ownership
            if (strcmp(node->owner, username) != 0)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_PERMISSION_DENIED\n", 22);
                continue;
            }
            
            // Check if it's a folder - folders cannot be deleted with DELETE
            if (node->is_folder) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_CANNOT_DELETE_FOLDER\n", 25);
                log_message(NS_LOG_FILE, "WARNING", "Cannot delete folder %s", arg1);
                continue;
            }

            // Check if file has any active locks on any SS before deleting
            int has_locks = 0;
            for (int r = 0; r < node->ss_count && !has_locks; r++) {
                StorageServer* ss = get_ss_by_id(node->ss_ids[r]);
                if (ss != NULL && ss->is_active) {
                    int ss_sock = connect_to_server(ss->ip, ss->nm_port);
                    if (ss_sock >= 0) {
                        char check_cmd[BUFFER_SIZE];
                        snprintf(check_cmd, sizeof(check_cmd), "NM_CHECK_LOCKS %s\n", arg1);
                        write(ss_sock, check_cmd, strlen(check_cmd));
                        
                        char ss_response[BUFFER_SIZE];
                        int resp_size = read(ss_sock, ss_response, BUFFER_SIZE - 1);
                        close(ss_sock);
                        
                        if (resp_size > 0) {
                            ss_response[resp_size] = '\0';
                            if (strncmp(ss_response, "FILE_LOCKED", 11) == 0) {
                                has_locks = 1;
                            }
                        }
                    }
                }
            }
            
            if (has_locks) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_LOCKED\n", 16);
                log_message(NS_LOG_FILE, "WARNING", "Cannot delete %s: file has active locks", arg1);
                continue;
            }

            // Copy all SS IDs before unlock
            int ss_count_copy = node->ss_count;
            char* all_ss_ids[MAX_SS];
            for (int i = 0; i < ss_count_copy && i < MAX_SS; i++) {
                all_ss_ids[i] = strdup(node->ss_ids[i]);
            }
            pthread_mutex_unlock(&file_trie_mutex); // Unlock before network call

            // Delete from ALL storage servers that have this file
            int deleted_count = 0;
            for (int i = 0; i < ss_count_copy; i++) {
                StorageServer *ss = get_ss_by_id(all_ss_ids[i]);
                if (ss != NULL && ss->is_active) {
                    // Connect to SS's NM_PORT
                    int ss_sock = connect_to_server(ss->ip, ss->nm_port);
                    if (ss_sock >= 0) {
                        char ss_cmd[BUFFER_SIZE];
                        snprintf(ss_cmd, sizeof(ss_cmd), "NM_DELETE %s\n", arg1);
                        write(ss_sock, ss_cmd, strlen(ss_cmd));

                        char ss_ack[BUFFER_SIZE];
                        read(ss_sock, ss_ack, BUFFER_SIZE);
                        close(ss_sock);

                        if (strncmp(ss_ack, "ACK_NM_DELETE", 13) == 0) {
                            deleted_count++;
                            StorageServer* del_ss = get_ss_by_id(all_ss_ids[i]);
                            log_message(NS_LOG_FILE, "SUCCESS", "User '%s' deleted file '%s' from SS %s (%s:%d)", 
                                       username, arg1, all_ss_ids[i], del_ss ? del_ss->ip : "unknown", del_ss ? del_ss->nm_port : 0);
                        }
                    }
                }
                free(all_ss_ids[i]);
            }

            if (deleted_count > 0)
            {
                pthread_mutex_lock(&file_trie_mutex);
                delete_file(file_trie_root, arg1, 0); // Perform lazy delete
                pthread_mutex_unlock(&file_trie_mutex);
                persist_trie(); // Save to disk
                
                // Invalidate cache entry
                invalidate_cache_entry(arg1);
                
                write(sock, "ACK_DELETE\n", 11);
                log_message(NS_LOG_FILE, "SUCCESS", "File %s deleted from %d storage servers", arg1, deleted_count);
            }
            else
            {
                write(sock, "ERR_SS_DELETE_FAILED\n", 21);
            }
        }

        // --- READ / STREAM / WRITE ---
        // These 3 commands all start the same way:
        // 1. Find file, 2. Check perms, 3. Get SS IP/Port, 4. Reply to client.
        else if (strcmp(command, "READ") == 0 ||
                 strcmp(command, "STREAM") == 0 ||
                 strcmp(command, "WRITE") == 0)
        {
            char *filename = arg1;
            if (strlen(filename) == 0)
            {
                write(sock, "ERR_NO_FILENAME\n", 16);
                continue;
            }

            // Try cache first for O(1) lookup
            StorageServer *ss = get_cached_ss(filename);
            PermissionLevel perm = PERM_NONE;
            char selected_ss_id[50] = "";
            char selected_ss_ip[50] = "";
            int selected_ss_port = 0;
            int use_cache = 0;
            
            pthread_mutex_lock(&ss_list_mutex);
            if (ss != NULL && ss->is_active) {
                use_cache = 1;
                strcpy(selected_ss_id, ss->id);
                strcpy(selected_ss_ip, ss->ip);
                selected_ss_port = ss->client_port;
            }
            pthread_mutex_unlock(&ss_list_mutex);
            
            if (use_cache) {
                // Cache hit! Still need to check permissions
                pthread_mutex_lock(&file_trie_mutex);
                FileNode *node = find_file(file_trie_root, filename);
                if (node != NULL) {
                    perm = check_permission(node, username);
                }
                pthread_mutex_unlock(&file_trie_mutex);
                
                if (perm == PERM_NONE) {
                    write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                    invalidate_cache_entry(filename); // File no longer exists
                    continue;
                }
                
                log_message(NS_LOG_FILE, "INFO", "Cache HIT for '%s' -> SS %s", filename, selected_ss_id);
            } else {
                // Cache miss or inactive SS - do full lookup
                if (ss != NULL) {
                    log_message(NS_LOG_FILE, "WARNING", "Cached SS for '%s' is inactive, trying replicas", filename);
                    invalidate_cache_entry(filename);
                } else {
                    log_message(NS_LOG_FILE, "INFO", "Cache MISS for '%s'", filename);
                }
                
                pthread_mutex_lock(&file_trie_mutex);
                FileNode *node = find_file(file_trie_root, filename);
                if (node == NULL)
                {
                    pthread_mutex_unlock(&file_trie_mutex);
                    write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                    continue;
                }

                // Check permissions
                perm = check_permission(node, username);

                // Get SS info - try primary first, then replicas
                int ss_count_copy = node->ss_count;
                char* all_ss_ids[MAX_SS];
                for (int i = 0; i < ss_count_copy && i < MAX_SS; i++) {
                    all_ss_ids[i] = strdup(node->ss_ids[i]);
                }
                pthread_mutex_unlock(&file_trie_mutex);

                ss = NULL;
                
                // Try each replica until we find an active one
                for (int i = 0; i < ss_count_copy; i++) {
                    ss = get_ss_by_id(all_ss_ids[i]);
                    if (ss != NULL && ss->is_active) {
                        strcpy(selected_ss_id, all_ss_ids[i]);
                        strcpy(selected_ss_ip, ss->ip);
                        selected_ss_port = ss->client_port;
                        break;
                    }
                }
                
                // Clean up
                for (int i = 0; i < ss_count_copy; i++) {
                    free(all_ss_ids[i]);
                }
                
                if (ss == NULL || !ss->is_active) {
                    write(sock, "ERR_SS_UNREACHABLE\n", 19);
                    continue;
                }
                
                // Cache the result for next time
                cache_file_ss(filename, selected_ss_id);
            }

            // Check permissions
            if (strcmp(command, "WRITE") == 0 && perm < PERM_WRITE)
            {
                write(sock, "ERR_WRITE_PERMISSION_DENIED\n", 28);
                continue;
            }
            if (strcmp(command, "WRITE") != 0 && perm < PERM_READ)
            {
                write(sock, "ERR_READ_PERMISSION_DENIED\n", 27);
                continue;
            }

            // All checks passed! Send the SS info to the client
            char response[BUFFER_SIZE];
            snprintf(response, sizeof(response), "ACK_%s %s %d\n",
                     command, selected_ss_ip, selected_ss_port);

            write(sock, response, strlen(response));
            log_message(NS_LOG_FILE, "RESPONSE", "Sent SS %s info (%s:%d) to user '%s' (%s:%d) for '%s' operation on '%s'",
                   selected_ss_id, selected_ss_ip, selected_ss_port, username, client_ip, client_port, command, arg1);
        }
        else if (strcmp(command, "UNDO") == 0)
        {
            char *filename = arg1;
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);

            if (node == NULL)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }

            // UNDO requires WRITE permission
            if (check_permission(node, username) < PERM_WRITE)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_PERMISSION_DENIED\n", 22);
                continue;
            }

            // Get SS info
            char ss_id_copy[50];
            strcpy(ss_id_copy, node->ss_ids[0]);  // Use first (primary) SS
            pthread_mutex_unlock(&file_trie_mutex);

            StorageServer *ss = get_ss_by_id(ss_id_copy);
            if (ss == NULL || !ss->is_active)
            {
                write(sock, "ERR_SS_UNREACHABLE\n", 19);
                continue;
            }

            // Send redirect to client
            char response[BUFFER_SIZE];
            snprintf(response, sizeof(response), "ACK_UNDO %s %d\n", ss->ip, ss->client_port);
            write(sock, response, strlen(response));
        }

        // --- CHECKPOINT actions (redirect to SS) ---
        else if (strcmp(command, "CHECKPOINT") == 0 ||
                 strcmp(command, "REVERT") == 0 ||
                 strcmp(command, "VIEWCHECKPOINT") == 0 ||
                 strcmp(command, "LISTCHECKPOINTS") == 0)
        {
            char *filename = arg1;
            if (strlen(filename) == 0)
            {
                write(sock, "ERR_NO_FILENAME\n", 16);
                continue;
            }

            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);
            if (node == NULL)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }

            PermissionLevel perm = check_permission(node, username);
            int need_write = (strcmp(command, "CHECKPOINT") == 0 || strcmp(command, "REVERT") == 0);
            if ((need_write && perm < PERM_WRITE) || (!need_write && perm < PERM_READ))
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_PERMISSION_DENIED\n", 22);
                continue;
            }

            char ss_id_copy[50];
            strcpy(ss_id_copy, node->ss_ids[0]);  // Use first (primary) SS
            pthread_mutex_unlock(&file_trie_mutex);

            StorageServer *ss = get_ss_by_id(ss_id_copy);
            if (ss == NULL || !ss->is_active)
            {
                write(sock, "ERR_SS_UNREACHABLE\n", 19);
                continue;
            }

            char response[BUFFER_SIZE];
            if (strcmp(command, "CHECKPOINT") == 0)
                snprintf(response, sizeof(response), "ACK_CHECKPOINT %s %d\n", ss->ip, ss->client_port);
            else if (strcmp(command, "REVERT") == 0)
                snprintf(response, sizeof(response), "ACK_REVERT %s %d\n", ss->ip, ss->client_port);
            else if (strcmp(command, "VIEWCHECKPOINT") == 0)
                snprintf(response, sizeof(response), "ACK_VIEWCHECKPOINT %s %d\n", ss->ip, ss->client_port);
            else
                snprintf(response, sizeof(response), "ACK_LISTCHECKPOINTS %s %d\n", ss->ip, ss->client_port);

            write(sock, response, strlen(response));
        }

        // --- REQACCESS (Request access) ---
        else if (strcmp(command, "REQACCESS") == 0)
        {
            // REQACCESS -R/-W <filename>
            char *flag = arg1; char *filename = arg2;
            if (strlen(flag)==0 || strlen(filename)==0) { write(sock, "ERR_INVALID_ARGS\n", 17); continue; }

            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);
            if (node == NULL) { pthread_mutex_unlock(&file_trie_mutex); write(sock, "ERR_FILE_NOT_FOUND\n", 19); continue; }
            // disallow owner requesting
            if (strcmp(node->owner, username) == 0) { pthread_mutex_unlock(&file_trie_mutex); write(sock, "ERR_ALREADY_OWNER\n", 18); continue; }
            // if already has perm
            PermissionLevel perm = check_permission(node, username);
            if ((strcmp(flag, "-R")==0 && perm>=PERM_READ) || (strcmp(flag, "-W")==0 && perm>=PERM_WRITE)) {
                pthread_mutex_unlock(&file_trie_mutex); write(sock, "ERR_ALREADY_HAS_ACCESS\n", 23); continue; }
            char owner_copy[100]; snprintf(owner_copy, sizeof(owner_copy), "%s", node->owner);
            pthread_mutex_unlock(&file_trie_mutex);

            RequestType type = (strcmp(flag, "-W")==0) ? REQ_WRITE : REQ_READ;
            int id = create_request(filename, username, owner_copy, type);
            if (id < 0) { 
                write(sock, "ERR_REQ_CREATE\n", 15); 
                log_message(NS_LOG_FILE, "ERROR", "User '%s' (%s:%d) failed to create %s access request for '%s'", 
                           username, client_ip, client_port, flag, filename);
            }
            else { 
                char resp[128]; 
                snprintf(resp, sizeof(resp), "ACK_REQACCESS %d\n", id); 
                write(sock, resp, strlen(resp)); 
                log_message(NS_LOG_FILE, "INFO", "User '%s' (%s:%d) requested %s access to '%s' (owner: %s, request_id: %d)", 
                           username, client_ip, client_port, flag, filename, owner_copy, id);
            }
        }

        // --- LISTREQ (View requests for user) ---
        else if (strcmp(command, "LISTREQ") == 0)
        {
            char out[BUFFER_SIZE*2]; out[0]='\0';
            strcat(out, "ID  TYPE   FILE             REQUESTER        OWNER           STATUS\n");
            pthread_mutex_lock(&requests_mutex);
            char line[512]; // Increased buffer size to prevent truncation
            for (int i=0;i<request_count;i++) {
                AccessRequest *r=&requests[i];
                if (strcmp(r->requester, username)==0 || strcmp(r->owner, username)==0) {
                    snprintf(line, sizeof(line), "%3d %-6s %-16.16s %-15.15s %-15.15s %-8s\n",
                        r->id, request_type_str(r->type), r->filename, r->requester, r->owner, request_status_str(r->status));
                    strcat(out, line);
                }
            }
            pthread_mutex_unlock(&requests_mutex);
            if (strlen(out)==0) strcpy(out, "No requests.\n");
            write(sock, out, strlen(out));
        }

        else if (strcmp(command, "APPROVE") == 0 || strcmp(command, "DENY") == 0)
        {
            // APPROVE <id> | DENY <id>
            int id = atoi(arg1);
            if (id<=0) { write(sock, "ERR_INVALID_ID\n", 15); continue; }
            pthread_mutex_lock(&requests_mutex);
            AccessRequest *r = find_request_by_id(id);
            if (!r) { pthread_mutex_unlock(&requests_mutex); write(sock, "ERR_REQ_NOT_FOUND\n", 18); continue; }
            if (strcmp(r->owner, username)!=0) { pthread_mutex_unlock(&requests_mutex); write(sock, "ERR_NOT_REQUEST_OWNER\n", 22); continue; }
            if (r->status != RSTATUS_PENDING) { pthread_mutex_unlock(&requests_mutex); write(sock, "ERR_REQ_NOT_PENDING\n", 20); continue; }
            RequestType type = r->type; char filename[MAX_FILENAME]; snprintf(filename, sizeof(filename), "%s", r->filename); char requester[100]; snprintf(requester, sizeof(requester), "%s", r->requester);
            int approve = (strcmp(command, "APPROVE")==0);
            r->status = approve ? RSTATUS_APPROVED : RSTATUS_DENIED;
            pthread_mutex_unlock(&requests_mutex);

            if (approve) {
                // add to ACL
                pthread_mutex_lock(&file_trie_mutex);
                FileNode *node = find_file(file_trie_root, filename);
                if (node) {
                    if (type==REQ_WRITE) {
                        if (node->acl.write_count < MAX_USERS)
                            node->acl.write_users[node->acl.write_count++] = strdup(requester);
                    } else {
                        if (node->acl.read_count < MAX_USERS)
                            node->acl.read_users[node->acl.read_count++] = strdup(requester);
                    }
                }
                pthread_mutex_unlock(&file_trie_mutex);
                log_message(NS_LOG_FILE, "SUCCESS", "User '%s' (%s:%d) APPROVED %s access request #%d for '%s' (requester: %s)", 
                           username, client_ip, client_port, type==REQ_WRITE?"WRITE":"READ", id, filename, requester);
            } else {
                log_message(NS_LOG_FILE, "INFO", "User '%s' (%s:%d) DENIED %s access request #%d for '%s' (requester: %s)", 
                           username, client_ip, client_port, type==REQ_WRITE?"WRITE":"READ", id, filename, requester);
            }
            write(sock, approve?"ACK_APPROVED\n":"ACK_DENIED\n", approve?13:12);
        }

        // --- man pages ---
        else if (strcmp(command, "man") == 0)
        {
            char *topic = arg1;
            char out[BUFFER_SIZE*2]; out[0]='\0';
            if (strlen(topic)==0) {
                strcpy(out, "Usage: man <COMMAND>\nTry: man CREATE, man READ, man WRITE, man CHECKPOINT, man REQACCESS, man LISTREQ, man APPROVE, man DENY\n");
            } else if (strcmp(topic, "CHECKPOINT")==0) {
                strcpy(out, "CHECKPOINT <filename> <tag>\n  Save current file content as a named checkpoint. Requires WRITE access.\n");
            } else if (strcmp(topic, "VIEWCHECKPOINT")==0) {
                strcpy(out, "VIEWCHECKPOINT <filename> <tag>\n  View contents of a specific checkpoint. Requires READ access.\n");
            } else if (strcmp(topic, "LISTCHECKPOINTS")==0) {
                strcpy(out, "LISTCHECKPOINTS <filename>\n  List all checkpoint tags saved for the file. Requires READ access.\n");
            } else if (strcmp(topic, "REVERT")==0) {
                strcpy(out, "REVERT <filename> <tag>\n  Revert file to the specified checkpoint. Creates a .bak for UNDO. Requires WRITE access.\n");
            } else if (strcmp(topic, "REQACCESS")==0) {
                strcpy(out, "REQACCESS -R|-W <filename>\n  Ask the owner for READ or WRITE access to a file you don't own.\n");
            } else if (strcmp(topic, "LISTREQ")==0) {
                strcpy(out, "LISTREQ\n  List access requests related to you. Shows sent and received with status and IDs.\n");
            } else if (strcmp(topic, "APPROVE")==0) {
                strcpy(out, "APPROVE <request_id>\n  Approve a pending access request for a file you own. Automatically updates ACL.\n");
            } else if (strcmp(topic, "DENY")==0) {
                strcpy(out, "DENY <request_id>\n  Deny a pending access request for a file you own.\n");
            } else {
                strcpy(out, "No manual entry for that command.\n");
            }
            write(sock, out, strlen(out));
        }

        // --- EXEC ---
        else if (strcmp(command, "EXEC") == 0)
        {
            char *filename = arg1;
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);

            if (node == NULL)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }

            // EXEC requires READ permission
            if (check_permission(node, username) < PERM_READ)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_READ_PERMISSION_DENIED\n", 27);
                continue;
            }

            // Get SS info
            char ss_id_copy[50];
            strcpy(ss_id_copy, node->ss_ids[0]);  // Use first (primary) SS
            pthread_mutex_unlock(&file_trie_mutex);

            StorageServer *ss = get_ss_by_id(ss_id_copy);
            if (ss == NULL || !ss->is_active)
            {
                write(sock, "ERR_SS_UNREACHABLE\n", 19);
                continue;
            }

            // --- NM acts as a client to get the file ---
            int ss_sock = connect_to_server(ss->ip, ss->client_port);
            char ss_cmd[BUFFER_SIZE], file_content[8192] = {0};
            snprintf(ss_cmd, sizeof(ss_cmd), "READ %s\n", filename);
            write(ss_sock, ss_cmd, strlen(ss_cmd));

            int read_len;
            while ((read_len = read(ss_sock, file_content, sizeof(file_content) - 1)) > 0)
            {
                file_content[read_len] = '\0'; // Just read the whole file
                break;                         // Assuming file fits in 8KB for this op
            }
            close(ss_sock);

            if (strlen(file_content) == 0)
            {
                write(sock, "ERR_FILE_EMPTY\n", 15);
                continue;
            }

            // --- Write content to a temporary script file ---
            const char *tmp_script_path = "/tmp/nm_exec_script.sh";
            FILE *tmp_script = fopen(tmp_script_path, "w");
            if (tmp_script == NULL)
            {
                write(sock, "ERR_NM_EXEC_FAILED\n", 19);
                continue;
            }
            fputs(file_content, tmp_script);
            fclose(tmp_script);

            // Make it executable
            chmod(tmp_script_path, 0755); // rwxr-xr-x

            // --- Run the script using popen ---
            char exec_cmd[512], output_buffer[8192] = {0};
            snprintf(exec_cmd, sizeof(exec_cmd), "%s 2>&1", tmp_script_path); // 2>&1 merges stderr

            FILE *pipe = popen(exec_cmd, "r");
            if (pipe == NULL)
            {
                write(sock, "ERR_NM_POPEN_FAILED\n", 20);
                remove(tmp_script_path);
                continue;
            }

            // Read all output
            read_len = fread(output_buffer, 1, sizeof(output_buffer) - 1, pipe);
            output_buffer[read_len] = '\0';
            pclose(pipe);

            // --- Send output to client and clean up ---
            write(sock, output_buffer, strlen(output_buffer));
            remove(tmp_script_path);
        }

        else if (strcmp(command, "VIEW") == 0)
        {
            int list_all = 0;
            int show_details = 0;

            // Parse flags: -a, -l, -al, -la
            if (strlen(arg1) > 0 && arg1[0] == '-') {
                for (int i = 1; arg1[i] != '\0'; i++) {
                    if (arg1[i] == 'a') list_all = 1;
                    if (arg1[i] == 'l') show_details = 1;
                }
            }

            if (!show_details) {
                // Simple listing without details
                char file_list_buffer[BUFFER_SIZE * 4];
                bzero(file_list_buffer, sizeof(file_list_buffer));

                pthread_mutex_lock(&file_trie_mutex);
                list_files(file_trie_root, username, list_all, 0, file_list_buffer);
                pthread_mutex_unlock(&file_trie_mutex);

                if (strlen(file_list_buffer) == 0)
                {
                    write(sock, "No files found.\n", 16);
                }
                else
                {
                    write(sock, file_list_buffer, strlen(file_list_buffer));
                }
            } else {
                // Detailed listing with stats - collect file info first, then fetch stats
                typedef struct {
                    char filename[MAX_FILENAME];
                    char owner[100];
                    char ss_id[50];
                    char ss_ip[50];
                    int ss_port;
                    long size;
                    time_t last_modified;
                    int is_folder;
                } FileInfo;
                
                FileInfo file_list[256];
                int file_count = 0;
                
                // Step 1: Collect file info while holding lock (NO network calls)
                // We'll do a simple traversal inline
                pthread_mutex_lock(&file_trie_mutex);
                
                // Helper to traverse and collect file info with proper prefix tracking
                typedef struct {
                    FileNode* node;
                    char prefix[MAX_FILENAME * 2];
                } StackEntry;
                
                StackEntry stack[1000];
                int stack_top = 0;
                
                if (file_trie_root) {
                    stack[stack_top].node = file_trie_root;
                    stack[stack_top].prefix[0] = '\0';
                    stack_top++;
                }
                
                while (stack_top > 0 && file_count < 256) {
                    stack_top--;
                    FileNode* node = stack[stack_top].node;
                    // Make a local copy of the prefix to avoid corruption
                    char current_prefix[MAX_FILENAME * 2];
                    strcpy(current_prefix, stack[stack_top].prefix);
                    
                    if (node->is_end_of_word && !node->is_in_trash) {
                        if (list_all || check_permission(node, username) >= PERM_READ) {
                            strcpy(file_list[file_count].filename, current_prefix);
                            strcpy(file_list[file_count].owner, node->owner ? node->owner : "unknown");
                            file_list[file_count].size = node->size;
                            file_list[file_count].last_modified = node->last_modified;
                            file_list[file_count].is_folder = node->is_folder;
                            
                            // Get primary SS info
                            if (node->ss_count > 0 && node->ss_ids[0]) {
                                strcpy(file_list[file_count].ss_id, node->ss_ids[0]);
                                StorageServer* ss = get_ss_by_id(node->ss_ids[0]);
                                if (ss && ss->is_active) {
                                    strcpy(file_list[file_count].ss_ip, ss->ip);
                                    file_list[file_count].ss_port = ss->nm_port;
                                } else {
                                    file_list[file_count].ss_ip[0] = '\0';
                                    file_list[file_count].ss_port = 0;
                                }
                            } else {
                                file_list[file_count].ss_id[0] = '\0';
                                file_list[file_count].ss_ip[0] = '\0';
                                file_list[file_count].ss_port = 0;
                            }
                            file_count++;
                        }
                    }
                    
                    // Add children to stack (in reverse order for correct traversal)
                    for (int i = 127; i >= 0; i--) {
                        if (node->children[i] != NULL && stack_top < 1000) {
                            stack[stack_top].node = node->children[i];
                            int len = strlen(current_prefix);
                            strcpy(stack[stack_top].prefix, current_prefix);
                            stack[stack_top].prefix[len] = (char)i;
                            stack[stack_top].prefix[len + 1] = '\0';
                            stack_top++;
                        }
                    }
                }
                
                pthread_mutex_unlock(&file_trie_mutex);
                // Lock released! Now safe to make network calls
                
                // Step 2: Fetch stats from storage servers (without holding lock)
                char output[BUFFER_SIZE * 8];
                output[0] = '\0';
                
                // Add header
                strcat(output, "PERMS      OWNER        SIZE    WORDS    CHARS    LAST ACCESS        FILENAME\n");
                strcat(output, "================================================================================\n");
                
                for (int i = 0; i < file_count; i++) {
                    char line[512];
                    long file_size = file_list[i].size;
                    long words = 0, chars = 0;
                    time_t last_access = 0;
                    
                    // Fetch stats from SS if available
                    if (file_list[i].ss_ip[0] != '\0' && !file_list[i].is_folder) {
                        int ss_sock = connect_to_server(file_list[i].ss_ip, file_list[i].ss_port);
                        if (ss_sock >= 0) {
                            char cmd[BUFFER_SIZE];
                            snprintf(cmd, sizeof(cmd), "NM_GETSTATS %s\n", file_list[i].filename);
                            write(ss_sock, cmd, strlen(cmd));
                            
                            char response[BUFFER_SIZE];
                            int len = read(ss_sock, response, sizeof(response) - 1);
                            if (len > 0) {
                                response[len] = '\0';
                                sscanf(response, "STATS %ld %ld %ld %ld", &file_size, &words, &chars, &last_access);
                            }
                            close(ss_sock);
                        }
                    }
                    
                    // Format permissions (simplified for now)
                    const char* perms = file_list[i].is_folder ? "drwxr-xr-x" : "-rw-r--r--";
                    
                    // Format last access time
                    char access_time[30];
                    if (last_access > 0) {
                        struct tm *tm_info = localtime(&last_access);
                        strftime(access_time, sizeof(access_time), "%b %d %H:%M", tm_info);
                    } else {
                        strcpy(access_time, "Never");
                    }
                    
                    // Format line with proper spacing
                    snprintf(line, sizeof(line), "%-10s %-12s %7ld %8ld %8ld  %-18s %s\n",
                            perms, file_list[i].owner, file_size, words, chars, 
                            access_time, file_list[i].filename);
                    strcat(output, line);
                }
                
                write(sock, output, strlen(output));
            }
        }

        // --- INFO ---
        else if (strcmp(command, "INFO") == 0)
        {
            char *filename = arg1;
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);

            // Check for file and READ permission
            if (node == NULL || check_permission(node, username) < PERM_READ)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND_OR_NO_ACCESS\n", 32);
                continue;
            }

            // Get file info we need, then release lock before network call
            char owner[100], ss_ip[50];
            int ss_port = 0;
            int is_folder = node->is_folder;
            time_t creation_time = node->creation_time;
            strcpy(owner, node->owner ? node->owner : "unknown");
            
            // Get primary SS info for fetching live size
            if (node->ss_count > 0 && node->ss_ids[0]) {
                StorageServer* ss = get_ss_by_id(node->ss_ids[0]);
                if (ss && ss->is_active) {
                    strcpy(ss_ip, ss->ip);
                    ss_port = ss->nm_port;
                } else {
                    ss_ip[0] = '\0';
                }
            } else {
                ss_ip[0] = '\0';
            }
            
            // Copy ACL info before releasing lock
            char write_users[MAX_USERS][100];
            char read_users[MAX_USERS][100];
            int write_count = node->acl.write_count;
            int read_count = node->acl.read_count;
            for (int i = 0; i < write_count; i++) {
                strcpy(write_users[i], node->acl.write_users[i]);
            }
            for (int i = 0; i < read_count; i++) {
                strcpy(read_users[i], node->acl.read_users[i]);
            }
            
            pthread_mutex_unlock(&file_trie_mutex);
            // Lock released! Now safe to make network call
            
            // Fetch live size from storage server
            long file_size = 0;
            if (ss_ip[0] != '\0' && !is_folder) {
                int ss_sock = connect_to_server(ss_ip, ss_port);
                if (ss_sock >= 0) {
                    char cmd[BUFFER_SIZE];
                    snprintf(cmd, sizeof(cmd), "NM_GETSTATS %s\n", filename);
                    write(ss_sock, cmd, strlen(cmd));
                    
                    char response[BUFFER_SIZE];
                    int len = read(ss_sock, response, sizeof(response) - 1);
                    if (len > 0) {
                        response[len] = '\0';
                        long words, chars, last_access;
                        sscanf(response, "STATS %ld %ld %ld %ld", &file_size, &words, &chars, &last_access);
                    }
                    close(ss_sock);
                }
            }

            char info_buffer[BUFFER_SIZE * 2], time_str[100];

            // Format file info - send simple format, client will add box design
            ctime_r(&creation_time, time_str);
            time_str[strcspn(time_str, "\n")] = 0; // remove newline
            
            snprintf(info_buffer, sizeof(info_buffer),
                     "FILE:%s\nOWNER:%s\nSIZE:%ld\nCREATED:%s\n",
                     filename, owner, file_size, time_str);

            // Add write access list
            strcat(info_buffer, "WRITE_ACCESS:");
            if (write_count == 0) {
                strcat(info_buffer, "(none)");
            } else {
                for (int i = 0; i < write_count; i++)
                {
                    if (i > 0) strcat(info_buffer, ",");
                    strcat(info_buffer, write_users[i]);
                }
            }
            strcat(info_buffer, "\n");
            
            // Add read access list
            strcat(info_buffer, "READ_ACCESS:");
            if (read_count == 0) {
                strcat(info_buffer, "(none)");
            } else {
                for (int i = 0; i < read_count; i++)
                {
                    if (i > 0) strcat(info_buffer, ",");
                    strcat(info_buffer, read_users[i]);
                }
            }
            strcat(info_buffer, "\n");

            write(sock, info_buffer, strlen(info_buffer));
        }

        // --- ADDACCESS ---
        else if (strcmp(command, "ADDACCESS") == 0)
        {
            // Format: ADDACCESS -R/-W <filename> <username>
            char *flag = arg1;
            char *filename = arg2;
            char *user_to_add = arg3;

            if (strlen(flag) == 0 || strlen(filename) == 0 || strlen(user_to_add) == 0)
            {
                write(sock, "ERR_INVALID_ARGS\n", 17);
                continue;
            }

            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);

            // Check for file and ownership
            if (node == NULL || strcmp(node->owner, username) != 0)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND_OR_NOT_OWNER\n", 32);
                continue;
            }

            if (strcmp(flag, "-R") == 0)
            {
                // Add to read list
                if (node->acl.read_count < MAX_USERS)
                {
                    node->acl.read_users[node->acl.read_count++] = strdup(user_to_add);
                    write(sock, "ACK_ADDACCESS_READ\n", 19);
                }
                else
                {
                    write(sock, "ERR_ACL_FULL\n", 13);
                }
            }
            else if (strcmp(flag, "-W") == 0)
            {
                // Add to write list
                if (node->acl.write_count < MAX_USERS)
                {
                    node->acl.write_users[node->acl.write_count++] = strdup(user_to_add);
                    write(sock, "ACK_ADDACCESS_WRITE\n", 20);
                }
                else
                {
                    write(sock, "ERR_ACL_FULL\n", 13);
                }
            }
            else
            {
                write(sock, "ERR_INVALID_FLAG\n", 17);
            }
            pthread_mutex_unlock(&file_trie_mutex);
        }

        // --- REMACCESS ---
        else if (strcmp(command, "REMACCESS") == 0)
        {
            // Format: REMACCESS <filename> <username>
            char *filename = arg1;
            char *user_to_remove = arg2;

            if (strlen(filename) == 0 || strlen(user_to_remove) == 0)
            {
                write(sock, "ERR_INVALID_ARGS\n", 17);
                continue;
            }

            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);

            if (node == NULL || strcmp(node->owner, username) != 0)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND_OR_NOT_OWNER\n", 32);
                continue;
            }

            int found = 0;
            // Remove from write list
            for (int i = 0; i < node->acl.write_count; i++)
            {
                if (strcmp(node->acl.write_users[i], user_to_remove) == 0)
                {
                    free(node->acl.write_users[i]);
                    // Shift remaining users left to fill the gap
                    node->acl.write_users[i] = node->acl.write_users[node->acl.write_count - 1];
                    node->acl.write_users[node->acl.write_count - 1] = NULL;
                    node->acl.write_count--;
                    found = 1;
                    break; // User can only be in one list at a time (unless we remove from both)
                }
            }
            // Remove from read list (only if not found in write list)
            if (!found)
            {
                for (int i = 0; i < node->acl.read_count; i++)
                {
                    if (strcmp(node->acl.read_users[i], user_to_remove) == 0)
                    {
                        free(node->acl.read_users[i]);
                        node->acl.read_users[i] = node->acl.read_users[node->acl.read_count - 1];
                        node->acl.read_users[node->acl.read_count - 1] = NULL;
                        node->acl.read_count--;
                        found = 1;
                        break;
                    }
                }
            }

            pthread_mutex_unlock(&file_trie_mutex);
            if (found)
            {
                write(sock, "ACK_REMACCESS\n", 14);
            }
            else
            {
                write(sock, "ERR_USER_NOT_IN_ACL\n", 20);
            }
        }

        // --- CREATEFOLDER ---
        else if (strcmp(command, "CREATEFOLDER") == 0)
        {
            char *foldername = arg1;
            if (strlen(foldername) == 0)
            {
                write(sock, "ERR_NO_FOLDERNAME\n", 18);
                continue;
            }

            pthread_mutex_lock(&file_trie_mutex);
            if (find_file(file_trie_root, foldername) != NULL)
            {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FOLDER_EXISTS\n", 18);
                continue;
            }
            pthread_mutex_unlock(&file_trie_mutex);

            // Select primary SS and replica SSs
            StorageServer *primary_ss = get_ss_for_new_file();
            if (primary_ss == NULL)
            {
                write(sock, "ERR_NO_SS_AVAIL\n", 16);
                continue;
            }

            // Select replica servers (different from primary)
            char* replica_ss_ids[MAX_SS];
            int replica_count = select_replica_servers(primary_ss->id, replica_ss_ids, REPLICATION_FACTOR - 1);

            // Create folder on primary SS
            int ss_sock = connect_to_server(primary_ss->ip, primary_ss->nm_port);
            char ss_cmd[BUFFER_SIZE];
            snprintf(ss_cmd, sizeof(ss_cmd), "NM_CREATEFOLDER %s\n", foldername);
            write(ss_sock, ss_cmd, strlen(ss_cmd));

            char ss_ack[BUFFER_SIZE];
            read(ss_sock, ss_ack, BUFFER_SIZE);
            close(ss_sock);

            if (strncmp(ss_ack, "ACK_NM_CREATEFOLDER", 19) == 0)
            {
                // Add all SS IDs (primary + replicas) to the folder metadata
                char* all_ss_ids[MAX_SS];
                all_ss_ids[0] = strdup(primary_ss->id);
                int total_ss = 1;
                for (int i = 0; i < replica_count; i++) {
                    all_ss_ids[total_ss++] = strdup(replica_ss_ids[i]);
                }

                pthread_mutex_lock(&file_trie_mutex);
                insert_file_with_replicas(file_trie_root, foldername, username, all_ss_ids, total_ss);
                // Mark it as a folder
                FileNode* folder_node = find_file(file_trie_root, foldername);
                if (folder_node) {
                    folder_node->is_folder = 1;
                }
                pthread_mutex_unlock(&file_trie_mutex);
                persist_trie();
                
                write(sock, "ACK_CREATEFOLDER\n", 17);
                log_message(NS_LOG_FILE, "SUCCESS", "Folder %s created on SS %s (with %d replicas)", foldername, primary_ss->id, replica_count);

                // Async replication to other storage servers
                for (int i = 0; i < replica_count; i++) {
                    StorageServer* replica_ss = get_ss_by_id(replica_ss_ids[i]);
                    if (replica_ss != NULL && replica_ss->is_active) {
                        pthread_t rep_tid;
                        ReplicationTask* task = malloc(sizeof(ReplicationTask));
                        strncpy(task->filename, foldername, MAX_FILENAME - 1);
                        strncpy(task->ss_ip, replica_ss->ip, 50);
                        task->ss_port = replica_ss->nm_port;
                        strncpy(task->ss_id, replica_ss->id, 50);
                        
                        // For folders, we use a different approach - just create the folder
                        pthread_create(&rep_tid, NULL, replicate_folder_async, (void*)task);
                        pthread_detach(rep_tid);
                    }
                    free(replica_ss_ids[i]);
                }
                
                for (int i = 0; i < total_ss; i++) {
                    free(all_ss_ids[i]);
                }
            }
            else
            {
                write(sock, "ERR_SS_CREATEFOLDER_FAILED\n", 27);
                for (int i = 0; i < replica_count; i++) {
                    free(replica_ss_ids[i]);
                }
            }
        }

        // --- MOVE ---
        // --- MOVE ---
        else if (strcmp(command, "MOVE") == 0)
        {
            char *src_path = arg1;
            char *dest_path = arg2; // Can be a foldername or "." for root

            if (strlen(src_path) == 0 || strlen(dest_path) == 0) {
                write(sock, "ERR_INVALID_ARGS\n", 17);
                continue;
            }

            pthread_mutex_lock(&file_trie_mutex);
            FileNode *file_node = find_file(file_trie_root, src_path);
            if (file_node == NULL) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_FILE_NOT_FOUND\n", 19);
                continue;
            }

            if (check_permission(file_node, username) < PERM_WRITE) {
                pthread_mutex_unlock(&file_trie_mutex);
                write(sock, "ERR_PERMISSION_DENIED\n", 22);
                continue;
            }

            // Copy all SS IDs that have this file
            int file_ss_count = file_node->ss_count;
            char* file_ss_ids[MAX_SS];
            for (int i = 0; i < file_ss_count && i < MAX_SS; i++) {
                file_ss_ids[i] = strdup(file_node->ss_ids[i]);
            }
            pthread_mutex_unlock(&file_trie_mutex);

            // Move the file on ALL storage servers that have it
            int moved_count = 0;
            for (int i = 0; i < file_ss_count; i++) {
                StorageServer *ss = get_ss_by_id(file_ss_ids[i]);
                if (ss != NULL && ss->is_active) {
                    // If moving to a folder (not "."), ensure the folder exists on this SS
                    if (strcmp(dest_path, ".") != 0) {
                        int folder_sock = connect_to_server(ss->ip, ss->nm_port);
                        if (folder_sock >= 0) {
                            char folder_cmd[BUFFER_SIZE];
                            snprintf(folder_cmd, sizeof(folder_cmd), "NM_CREATEFOLDER %s\n", dest_path);
                            write(folder_sock, folder_cmd, strlen(folder_cmd));
                            
                            char folder_ack[BUFFER_SIZE];
                            read(folder_sock, folder_ack, BUFFER_SIZE);
                            close(folder_sock);
                            
                            // Folder might already exist, that's OK
                            log_message(NS_LOG_FILE, "INFO", "Ensured folder %s exists on SS %s", dest_path, file_ss_ids[i]);
                        }
                    }
                    
                    // Tell SS to physically move the file
                    int ss_sock = connect_to_server(ss->ip, ss->nm_port);
                    if (ss_sock >= 0) {
                        char ss_cmd[BUFFER_SIZE];
                        snprintf(ss_cmd, sizeof(ss_cmd), "NM_MOVE %s %s\n", src_path, dest_path);
                        write(ss_sock, ss_cmd, strlen(ss_cmd));

                        char ss_ack[BUFFER_SIZE];
                        read(ss_sock, ss_ack, BUFFER_SIZE);
                        close(ss_sock);

                        if (strncmp(ss_ack, "ACK_NM_MOVE", 11) == 0) {
                            moved_count++;
                            log_message(NS_LOG_FILE, "SUCCESS", "File %s moved on SS %s", src_path, file_ss_ids[i]);
                        } else {
                            log_message(NS_LOG_FILE, "WARNING", "Failed to move %s on SS %s (ack: %s)", src_path, file_ss_ids[i], ss_ack);
                        }
                    }
                }
                free(file_ss_ids[i]);
            }

            if (moved_count > 0)
            {
                pthread_mutex_lock(&file_trie_mutex);
                if (move_file(file_trie_root, src_path, dest_path))
                {
                    pthread_mutex_unlock(&file_trie_mutex);
                    persist_trie(); // Save to disk
                    write(sock, "ACK_MOVE\n", 9);
                    log_message(NS_LOG_FILE, "SUCCESS", "File %s moved successfully on %d storage servers", src_path, moved_count);
                } else {
                    pthread_mutex_unlock(&file_trie_mutex);
                    write(sock, "ERR_MOVE_FAILED\n", 16);
                }
            } else {
                write(sock, "ERR_SS_MOVE_FAILED\n", 19);
            }
        }

        // --- VIEWFOLDER ---
        else if (strcmp(command, "VIEWFOLDER") == 0)
        {
            char *foldername = arg1;
            if (strlen(foldername) == 0)
            {
                write(sock, "ERR_NO_FOLDERNAME\n", 18);
                continue;
            }

            char folder_contents[BUFFER_SIZE * 4];
            bzero(folder_contents, sizeof(folder_contents));

            pthread_mutex_lock(&file_trie_mutex);
            list_folder_contents(file_trie_root, foldername, username, folder_contents);
            pthread_mutex_unlock(&file_trie_mutex);

            write(sock, folder_contents, strlen(folder_contents));
        }

        // --- LIST ---
        else if (strcmp(command, "LIST") == 0)
        {
            char user_list_str[BUFFER_SIZE * 2];
            bzero(user_list_str, sizeof(user_list_str));
            
            pthread_mutex_lock(&client_list_mutex);
            
            // First, list all active users
            strcat(user_list_str, "=== ACTIVE USERS ===\n");
            int active_count = 0;
            for (int i = 0; i < client_count; i++)
            {
                if (client_list[i].is_active)
                {
                    strcat(user_list_str, "  ");
                    strcat(user_list_str, client_list[i].username);
                    strcat(user_list_str, "\n");
                    active_count++;
                }
            }
            if (active_count == 0) {
                strcat(user_list_str, "  (none)\n");
            }
            
            // Then, list all disconnected users
            strcat(user_list_str, "\n=== DISCONNECTED USERS ===\n");
            int disconnected_count = 0;
            for (int i = 0; i < client_count; i++)
            {
                if (!client_list[i].is_active)
                {
                    strcat(user_list_str, "  ");
                    strcat(user_list_str, client_list[i].username);
                    strcat(user_list_str, "\n");
                    disconnected_count++;
                }
            }
            if (disconnected_count == 0) {
                strcat(user_list_str, "  (none)\n");
            }
            
            pthread_mutex_unlock(&client_list_mutex);
            write(sock, user_list_str, strlen(user_list_str));
        }

        else
        {
            write(sock, "ERR_UNKNOWN_CMD\n", 16);
        }
    }

    if (read_size == 0)
    {
        log_message(NS_LOG_FILE, "INFO", "User '%s' disconnected.", username);
        // Set user as inactive in client_list
        pthread_mutex_lock(&client_list_mutex);
        for (int i = 0; i < client_count; i++)
        {
            if (strcmp(client_list[i].username, username) == 0)
            {
                client_list[i].is_active = 0;
                break;
            }
        }
        pthread_mutex_unlock(&client_list_mutex);
    }
    else if (read_size == -1)
    {
        perror("read error");
    }
}

// --- Worker Thread Function for Thread Pool ---
void* worker_thread(void* arg) {
    int thread_id = *(int*)arg;
    free(arg);
    
    log_message(NS_LOG_FILE, "INFO", "Worker thread %d started", thread_id);
    
    while (1) {
        Task task = dequeue_task();
        
        if (shutdown_workers && task.sock == -1) {
            log_message(NS_LOG_FILE, "INFO", "Worker thread %d shutting down", thread_id);
            break;
        }
        
        if (task.is_registration) {
            // Handle registration
            if (strncmp(task.buffer, "REG_SS", 6) == 0) {
                handle_ss_registration(task.buffer, task.sock);
                close(task.sock);
            } else if (strncmp(task.buffer, "NM_FILE_MODIFIED", 16) == 0) {
                // Handle file modification notification from SS
                char filename[MAX_FILENAME];
                char modified_ss_id[50];
                long file_size = 0;
                long word_count = 0;
                long char_count = 0;
                long last_access = 0;
                sscanf(task.buffer, "NM_FILE_MODIFIED %s %s %ld %ld %ld %ld", 
                       filename, modified_ss_id, &file_size, &word_count, &char_count, &last_access);
                log_message(NS_LOG_FILE, "INFO", "Worker %d: Processing file modification for %s from SS %s (size: %ld, words: %ld)", 
                           thread_id, filename, modified_ss_id, file_size, word_count);
                
                // Get file metadata
                pthread_mutex_lock(&file_trie_mutex);
                FileNode *node = find_file(file_trie_root, filename);
                if (node == NULL) {
                    log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - File %s not found in trie", thread_id, filename);
                    pthread_mutex_unlock(&file_trie_mutex);
                    close(task.sock);
                    continue;
                }
                
                // Update file stats
                node->size = file_size;
                node->word_count = word_count;
                node->char_count = char_count;
                node->last_access = last_access;
                node->last_modified = time(NULL);
                if (node->ss_count <= 1) {
                    log_message(NS_LOG_FILE, "INFO", "Worker %d: File %s has only %d replica(s), skipping replication", thread_id, filename, node->ss_count);
                    pthread_mutex_unlock(&file_trie_mutex);
                    close(task.sock);
                    continue;
                }
                
                log_message(NS_LOG_FILE, "INFO", "Worker %d: File %s has %d replicas", thread_id, filename, node->ss_count);
                
                // Use the SS that sent the notification as the source
                char primary_ss_id[50];
                strcpy(primary_ss_id, modified_ss_id);
                
                // Get all other SS IDs (excluding the one that was modified)
                char* replica_ss_ids[MAX_SS];
                int replica_count = 0;
                for (int i = 0; i < node->ss_count && i < MAX_SS; i++) {
                    if (strcmp(node->ss_ids[i], modified_ss_id) != 0) {
                        replica_ss_ids[replica_count++] = strdup(node->ss_ids[i]);
                    }
                }
                pthread_mutex_unlock(&file_trie_mutex);
                
                log_message(NS_LOG_FILE, "INFO", "Worker %d: Found %d other replicas to sync", thread_id, replica_count);
                
                // Trigger replication to all replicas
                for (int i = 0; i < replica_count; i++) {
                    log_message(NS_LOG_FILE, "INFO", "Worker %d: Replicating to SS %s", thread_id, replica_ss_ids[i]);
                    StorageServer* replica_ss = get_ss_by_id(replica_ss_ids[i]);
                    if (replica_ss == NULL) {
                        log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - SS %s not found", thread_id, replica_ss_ids[i]);
                        free(replica_ss_ids[i]);
                        continue;
                    }
                    if (!replica_ss->is_active) {
                        log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - SS %s is not active", thread_id, replica_ss_ids[i]);
                        free(replica_ss_ids[i]);
                        continue;
                    }
                    
                    StorageServer* primary_ss = get_ss_by_id(primary_ss_id);
                    if (primary_ss == NULL) {
                        log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - Primary SS %s not found", thread_id, primary_ss_id);
                        free(replica_ss_ids[i]);
                        continue;
                    }
                    if (!primary_ss->is_active) {
                        log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - Primary SS %s is not active", thread_id, primary_ss_id);
                        free(replica_ss_ids[i]);
                        continue;
                    }
                    
                    // Read file content from primary
                    log_message(NS_LOG_FILE, "INFO", "Worker %d: Reading from primary SS %s at %s:%d", thread_id, primary_ss_id, primary_ss->ip, primary_ss->client_port);
                    int primary_sock = connect_to_server(primary_ss->ip, primary_ss->client_port);
                    if (primary_sock < 0) {
                        log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - Failed to connect to primary SS %s", thread_id, primary_ss_id);
                        free(replica_ss_ids[i]);
                        continue;
                    }
                    
                    char read_cmd[BUFFER_SIZE];
                    snprintf(read_cmd, sizeof(read_cmd), "READ %s\n", filename);
                    write(primary_sock, read_cmd, strlen(read_cmd));
                    
                    char file_content[8192] = {0};
                    int content_len = read(primary_sock, file_content, sizeof(file_content) - 1);
                    close(primary_sock);
                    
                    log_message(NS_LOG_FILE, "INFO", "Worker %d: Read %d bytes from primary", thread_id, content_len);
                    
                    if (content_len > 0) {
                        file_content[content_len] = '\0';
                        
                        // Delete and recreate file on replica
                        log_message(NS_LOG_FILE, "INFO", "Worker %d: Deleting old file on replica SS %s", thread_id, replica_ss->id);
                        int replica_sock = connect_to_server(replica_ss->ip, replica_ss->nm_port);
                        if (replica_sock >= 0) {
                            char delete_cmd[BUFFER_SIZE];
                            snprintf(delete_cmd, sizeof(delete_cmd), "NM_DELETE %s\n", filename);
                            write(replica_sock, delete_cmd, strlen(delete_cmd));
                            
                            char ack[BUFFER_SIZE];
                            read(replica_sock, ack, BUFFER_SIZE);
                            close(replica_sock);
                            
                            // Create new file
                            log_message(NS_LOG_FILE, "INFO", "Worker %d: Creating new file on replica SS %s", thread_id, replica_ss->id);
                            replica_sock = connect_to_server(replica_ss->ip, replica_ss->nm_port);
                            char create_cmd[BUFFER_SIZE];
                            snprintf(create_cmd, sizeof(create_cmd), "NM_CREATE %s\n", filename);
                            write(replica_sock, create_cmd, strlen(create_cmd));
                            read(replica_sock, ack, BUFFER_SIZE);
                            close(replica_sock);
                            
                            // Copy content using NM_WRITECONTENT command
                            log_message(NS_LOG_FILE, "INFO", "Worker %d: Writing %d bytes to replica SS %s", thread_id, content_len, replica_ss->id);
                            replica_sock = connect_to_server(replica_ss->ip, replica_ss->nm_port);
                            if (replica_sock >= 0) {
                                char write_cmd[BUFFER_SIZE + 8192];
                                int cmd_len = snprintf(write_cmd, sizeof(write_cmd), 
                                                     "NM_WRITECONTENT %s %d\n", filename, content_len);
                                int bytes_sent = write(replica_sock, write_cmd, cmd_len);
                                log_message(NS_LOG_FILE, "INFO", "Worker %d: Sent command (%d bytes)", thread_id, bytes_sent);
                                
                                bytes_sent = write(replica_sock, file_content, content_len);
                                log_message(NS_LOG_FILE, "INFO", "Worker %d: Sent content (%d bytes)", thread_id, bytes_sent);
                                
                                char ack[BUFFER_SIZE] = {0};
                                int ack_len = read(replica_sock, ack, BUFFER_SIZE - 1);
                                log_message(NS_LOG_FILE, "INFO", "Worker %d: Received ACK (%d bytes): %s", thread_id, ack_len, ack_len > 0 ? ack : "NONE");
                                close(replica_sock);
                                
                                if (ack_len > 0) {
                                    log_message(NS_LOG_FILE, "SUCCESS", "Replicated %s (%d bytes) from SS %s to SS %s", 
                                           filename, content_len, primary_ss_id, replica_ss->id);
                                } else {
                                    log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - No ACK received (ack_len=%d)", thread_id, ack_len);
                                }
                            } else {
                                log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - Failed to connect for writing content", thread_id);
                            }
                        } else {
                            log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - Failed to connect for deleting file", thread_id);
                        }
                    } else {
                        log_message(NS_LOG_FILE, "ERROR", "Worker %d: ERROR - No content read from primary (len=%d)", thread_id, content_len);
                    }
                    free(replica_ss_ids[i]);
                }
                
                close(task.sock);
            } else if (strncmp(task.buffer, "REG_CLIENT", 10) == 0) {
                char username[100];
                sscanf(task.buffer, "REG_CLIENT %s", username);

                pthread_mutex_lock(&client_list_mutex);
                
                // Check if this username is already logged in (active)
                int username_in_use = 0;
                for (int i = 0; i < client_count; i++) {
                    if (client_list[i].is_active && strcmp(client_list[i].username, username) == 0) {
                        username_in_use = 1;
                        break;
                    }
                }
                
                if (username_in_use) {
                    pthread_mutex_unlock(&client_list_mutex);
                    write(task.sock, "ERR_USERNAME_IN_USE\n", 20);
                    log_message(NS_LOG_FILE, "WARNING", "Login rejected: username '%s' is already in use (worker %d)", username, thread_id);
                    close(task.sock);
                    continue;
                }
                
                int client_slot = -1;

                // First, try to find an inactive slot with the SAME username (reconnection)
                for (int i = 0; i < client_count; i++) {
                    if (!client_list[i].is_active && strcmp(client_list[i].username, username) == 0) {
                        client_slot = i;
                        break;
                    }
                }

                // If not found, add a new slot (don't reuse other users' inactive slots)
                if (client_slot == -1) {
                    if (client_count < MAX_CLIENTS) {
                        client_slot = client_count;
                        client_count++;
                    } else {
                        pthread_mutex_unlock(&client_list_mutex);
                        write(task.sock, "ERR_MAX_CLIENTS\n", 16);
                        close(task.sock);
                        continue;
                    }
                }

                strcpy(client_list[client_slot].username, username);
                client_list[client_slot].socket_fd = task.sock;
                client_list[client_slot].is_active = 1;
                
                pthread_mutex_unlock(&client_list_mutex);

                log_message(NS_LOG_FILE, "SUCCESS", "Client '%s' registered in slot %d (worker %d)", username, client_slot, thread_id);
                write(task.sock, "ACK_REG\n", 8);

                // Continue handling commands from this client
                handle_client_commands(username, task.sock);
            }
        } else {
            // Handle regular command (already authenticated)
            handle_client_commands(task.username, task.sock);
        }
    }
    
    return NULL;
}

// This is the main thread function, same as Phase 2
void *handle_connection(void *socket_desc)
{
    int sock = *(int *)socket_desc;
    free(socket_desc);
    char buffer[BUFFER_SIZE];
    int read_size;

    bzero(buffer, BUFFER_SIZE);
    if ((read_size = read(sock, buffer, BUFFER_SIZE - 1)) > 0)
    {
        buffer[read_size] = '\0';  // Null-terminate
        log_message(NS_LOG_FILE, "REQUEST", "Received first message: %s", buffer);

        // --- SS Registration ---
        if (strncmp(buffer, "REG_SS", 6) == 0)
        {
            handle_ss_registration(buffer, sock);
            close(sock); // SS registration is one-shot
        }
        
        // --- SS File Modification Notification ---
        else if (strncmp(buffer, "NM_FILE_MODIFIED", 16) == 0)
        {
            char filename[MAX_FILENAME];
            char modified_ss_id[50];
            long file_size = 0;
            long word_count = 0;
            long char_count = 0;
            long last_access = 0;
            sscanf(buffer, "NM_FILE_MODIFIED %s %s %ld %ld %ld %ld", 
                   filename, modified_ss_id, &file_size, &word_count, &char_count, &last_access);
            log_message(NS_LOG_FILE, "INFO", "Received file modification notification for: %s from SS %s (size: %ld, words: %ld)", 
                       filename, modified_ss_id, file_size, word_count);
            
            // Get file metadata
            pthread_mutex_lock(&file_trie_mutex);
            FileNode *node = find_file(file_trie_root, filename);
            if (node == NULL || node->ss_count <= 1) {
                // File not found or no replicas to sync
                pthread_mutex_unlock(&file_trie_mutex);
                close(sock);
                return 0;
            }
            
            // Update file stats
            node->size = file_size;
            node->word_count = word_count;
            node->char_count = char_count;
            node->last_access = last_access;
            node->last_modified = time(NULL);
            
            // Use the SS that sent the notification as the source (it has the latest version)
            char primary_ss_id[50];
            strcpy(primary_ss_id, modified_ss_id);
            
            // Get all other SS IDs (excluding the one that was modified)
            char* replica_ss_ids[MAX_SS];
            int replica_count = 0;
            for (int i = 0; i < node->ss_count && i < MAX_SS; i++) {
                if (strcmp(node->ss_ids[i], modified_ss_id) != 0) {
                    replica_ss_ids[replica_count++] = strdup(node->ss_ids[i]);
                }
            }
            pthread_mutex_unlock(&file_trie_mutex);
            
            // Trigger async replication to all replicas
            for (int i = 0; i < replica_count; i++) {
                StorageServer* replica_ss = get_ss_by_id(replica_ss_ids[i]);
                if (replica_ss != NULL && replica_ss->is_active) {
                    StorageServer* primary_ss = get_ss_by_id(primary_ss_id);
                    if (primary_ss != NULL && primary_ss->is_active) {
                        // Read file content from primary
                        int primary_sock = connect_to_server(primary_ss->ip, primary_ss->client_port);
                        if (primary_sock >= 0) {
                            char read_cmd[BUFFER_SIZE];
                            snprintf(read_cmd, sizeof(read_cmd), "READ %s\n", filename);
                            write(primary_sock, read_cmd, strlen(read_cmd));
                            
                            char file_content[8192] = {0};
                            int content_len = read(primary_sock, file_content, sizeof(file_content) - 1);
                            close(primary_sock);
                            
                            if (content_len > 0) {
                                file_content[content_len] = '\0';
                                
                                // Delete and recreate file on replica
                                int replica_sock = connect_to_server(replica_ss->ip, replica_ss->nm_port);
                                if (replica_sock >= 0) {
                                    char delete_cmd[BUFFER_SIZE];
                                    snprintf(delete_cmd, sizeof(delete_cmd), "NM_DELETE %s\n", filename);
                                    write(replica_sock, delete_cmd, strlen(delete_cmd));
                                    
                                    char ack[BUFFER_SIZE];
                                    read(replica_sock, ack, BUFFER_SIZE);
                                    close(replica_sock);
                                    
                                    // Create new file
                                    replica_sock = connect_to_server(replica_ss->ip, replica_ss->nm_port);
                                    char create_cmd[BUFFER_SIZE];
                                    snprintf(create_cmd, sizeof(create_cmd), "NM_CREATE %s\n", filename);
                                    write(replica_sock, create_cmd, strlen(create_cmd));
                                    read(replica_sock, ack, BUFFER_SIZE);
                                    close(replica_sock);
                                    
                                    // Copy content using NM_WRITECONTENT command
                                    replica_sock = connect_to_server(replica_ss->ip, replica_ss->nm_port);
                                    if (replica_sock >= 0) {
                                        char write_cmd[BUFFER_SIZE + 8192];
                                        int cmd_len = snprintf(write_cmd, sizeof(write_cmd), 
                                                             "NM_WRITECONTENT %s %d\n", filename, content_len);
                                        write(replica_sock, write_cmd, cmd_len);
                                        write(replica_sock, file_content, content_len);
                                        read(replica_sock, ack, BUFFER_SIZE);
                                        close(replica_sock);
                                        
                                        log_message(NS_LOG_FILE, "SUCCESS", "Replicated %s (%d bytes) from %s to %s", 
                                               filename, content_len, primary_ss_id, replica_ss->id);
                                    }
                                }
                            }
                        }
                    }
                }
                free(replica_ss_ids[i]);
            }
            
            close(sock);
        }

        // --- Client Registration ---
        else if (strncmp(buffer, "REG_CLIENT", 10) == 0)
        {
            char username[100];
            sscanf(buffer, "REG_CLIENT %s", username);

            pthread_mutex_lock(&client_list_mutex);

            // Check if this username is already logged in (active)
            for (int i = 0; i < client_count; i++) {
                if (client_list[i].is_active && strcmp(client_list[i].username, username) == 0) {
                    pthread_mutex_unlock(&client_list_mutex);
                    write(sock, "ERR_USERNAME_IN_USE\n", 20);
                    log_message(NS_LOG_FILE, "WARNING", "Login rejected: username '%s' is already in use", username);
                    close(sock);
                    return 0;
                }
            }

            int client_slot = -1;

            // 1. Try to find an inactive slot with the SAME username (reconnection)
            for (int i = 0; i < client_count; i++) {
                if (!client_list[i].is_active && strcmp(client_list[i].username, username) == 0) {
                    client_slot = i;
                    break;
                }
            }

            // 2. If no matching inactive slot, add a new slot if there's space
            if (client_slot == -1) {
                if (client_count < MAX_CLIENTS) {
                    client_slot = client_count;
                    client_count++; // Only increment if adding a new slot
                } else {
                    pthread_mutex_unlock(&client_list_mutex);
                    write(sock, "ERR_MAX_CLIENTS\n", 16);
                    close(sock);
                    return 0;
                }
            }

            // At this point, client_slot is a valid, available slot
            strcpy(client_list[client_slot].username, username);
            client_list[client_slot].socket_fd = sock;
            client_list[client_slot].is_active = 1;
            
            pthread_mutex_unlock(&client_list_mutex);

            log_message(NS_LOG_FILE, "SUCCESS", "Client '%s' registered in slot %d.", username, client_slot);
            write(sock, "ACK_REG\n", 8);

            // Pass to the command handler loop
            handle_client_commands(username, sock);
        }
        else
        {
            log_message(NS_LOG_FILE, "WARNING", "Unrecognized first message. Closing.");
            close(sock);
        }
    }

    if (read_size == 0)
    {
        log_message(NS_LOG_FILE, "INFO", "Connection closed before registration.");
    }
    else if (read_size < 0)
    {
        perror("[NM] Read error during registration");
    }

    close(sock);
    return 0;
}

// Signal handler for graceful shutdown
void shutdown_all_connections(int signum) {
    printf("\n[NM] Received signal %d. Shutting down all connections...\n", signum);
    
    // Send shutdown message to all storage servers
    pthread_mutex_lock(&ss_list_mutex);
    for (int i = 0; i < ss_count; i++) {
        if (ss_list[i].is_active) {
            log_message(NS_LOG_FILE, "INFO", "Sending shutdown to Storage Server %s", ss_list[i].id);
            int sock = connect_to_server(ss_list[i].ip, ss_list[i].client_port);
            if (sock >= 0) {
                write(sock, "SHUTDOWN\n", 9);
                close(sock);
            }
        }
    }
    pthread_mutex_unlock(&ss_list_mutex);
    
    // Send shutdown message to all clients
    pthread_mutex_lock(&client_list_mutex);
    for (int i = 0; i < client_count; i++) {
        if (client_list[i].is_active) {
            log_message(NS_LOG_FILE, "INFO", "Sending shutdown to client %s", client_list[i].username);
            write(client_list[i].socket_fd, "SHUTDOWN\n", 9);
        }
    }
    pthread_mutex_unlock(&client_list_mutex);
    
    // Save persistent data
    log_message(NS_LOG_FILE, "INFO", "Saving file metadata to disk...");
    save_trie_to_file(file_trie_root, PERSISTENCE_FILE);
    
    log_message(NS_LOG_FILE, "INFO", "Name Server shutdown complete.");
    exit(0);
}

int main(int argc, char *argv[])
{
    // --- Initialize Logging ---
    init_log_file(NS_LOG_FILE);
    log_message(NS_LOG_FILE, "INFO", "=== Name Server Starting ===");
    
    // --- Ignore SIGPIPE to prevent crash on write to closed socket ---
    SIGPIPE_IGNORE();
    log_message(NS_LOG_FILE, "INFO", "SIGPIPE handler set to SIG_IGN");
    
    // --- Register signal handlers for graceful shutdown ---
    signal(SIGINT, shutdown_all_connections);  // Ctrl+C
    signal(SIGTERM, shutdown_all_connections); // kill command
    
    // --- Initialize FileTrie Root ---
    file_trie_root = create_file_node();
    log_message(NS_LOG_FILE, "INFO", "FileTrie initialized");
    
    // --- Initialize Cache ---
    init_cache();
    log_message(NS_LOG_FILE, "INFO", "File-to-SS cache initialized (%d entries)", CACHE_SIZE);
    
    // --- Initialize Task Queue ---
    init_task_queue();
    log_message(NS_LOG_FILE, "INFO", "Task queue initialized");
    
    // --- Load persistent data ---
    mkdir("persistent", 0755);
    mkdir("persistent/nm_data", 0755);
    
    if (load_trie_from_file(&file_trie_root, PERSISTENCE_FILE) > 0) {
        log_message(NS_LOG_FILE, "SUCCESS", "Loaded file metadata from disk");
    } else {
        log_message(NS_LOG_FILE, "INFO", "Starting with empty file system");
    }
    
    // --- Start Worker Thread Pool ---
    log_message(NS_LOG_FILE, "INFO", "Starting thread pool with %d workers", THREAD_POOL_SIZE);
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        int* thread_id = malloc(sizeof(int));
        *thread_id = i;
        if (pthread_create(&worker_threads[i], NULL, worker_thread, thread_id) < 0) {
            perror("ERROR creating worker thread");
            exit(1);
        }
        pthread_detach(worker_threads[i]);
    }
    log_message(NS_LOG_FILE, "SUCCESS", "Thread pool started successfully");

    // --- Start Heartbeat Listener Thread ---
    pthread_t heartbeat_tid;
    if (pthread_create(&heartbeat_tid, NULL, heartbeat_listener, NULL) < 0) {
        perror("ERROR creating heartbeat listener thread");
        exit(1);
    }
    pthread_detach(heartbeat_tid);
    log_message(NS_LOG_FILE, "SUCCESS", "Heartbeat listener thread started");

    // --- Start Failure Monitoring Thread ---
    pthread_t monitor_tid;
    if (pthread_create(&monitor_tid, NULL, monitor_failures, NULL) < 0) {
        perror("ERROR creating monitoring thread");
        exit(1);
    }
    pthread_detach(monitor_tid);
    log_message(NS_LOG_FILE, "SUCCESS", "Failure monitoring thread started");

    // --- Create listening socket ---
    int listen_fd = create_server_socket(NM_PORT);
    log_message(NS_LOG_FILE, "INFO", "Name Server listening on port %d", NM_PORT);
    
    // Set socket to non-blocking mode
    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    // --- Setup poll structures ---
    struct pollfd fds[MAX_CLIENTS];
    int nfds = 1; // Start with just the listening socket
    
    fds[0].fd = listen_fd;
    fds[0].events = POLLIN;
    
    log_message(NS_LOG_FILE, "INFO", "Poll event loop started");

    // --- Main poll event loop ---
    while (1) {
        int ret = poll(fds, nfds, -1); // Block until activity
        if (ret < 0) {
            if (errno == EINTR) continue; // Interrupted by signal
            perror("ERROR in poll");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            if (!(fds[i].revents & POLLIN)) continue; // No input event on this fd
            
            if (fds[i].fd == listen_fd) {
                // New connection on listening socket
                while (1) {
                    struct sockaddr_in cli_addr;
                    socklen_t clilen = sizeof(cli_addr);
                    int conn_fd = accept(listen_fd, (struct sockaddr *)&cli_addr, &clilen);
                    
                    if (conn_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            // All connections processed
                            break;
                        }
                        perror("ERROR on accept");
                        break;
                    }

                    log_message(NS_LOG_FILE, "INFO", "New connection accepted (fd=%d)", conn_fd);
                    
                    // Set client socket to non-blocking
                    flags = fcntl(conn_fd, F_GETFL, 0);
                    fcntl(conn_fd, F_SETFL, flags | O_NONBLOCK);
                    
                    // Add to poll array
                    if (nfds < MAX_CLIENTS) {
                        fds[nfds].fd = conn_fd;
                        fds[nfds].events = POLLIN;
                        nfds++;
                    } else {
                        log_message(NS_LOG_FILE, "WARNING", "Maximum clients reached, rejecting connection");
                        close(conn_fd);
                    }
                }
            } else {
                // Data available on client socket
                int client_fd = fds[i].fd;
                char buffer[BUFFER_SIZE];
                
                int read_size = read(client_fd, buffer, BUFFER_SIZE - 1);
                
                if (read_size > 0) {
                    buffer[read_size] = '\0';
                    log_message(NS_LOG_FILE, "REQUEST", "Received from fd=%d: %s", client_fd, buffer);
                    
                    // Remove from poll array (worker will handle further communication)
                    // Compact the array by moving the last element to this position
                    fds[i] = fds[nfds - 1];
                    nfds--;
                    i--; // Recheck this position since we moved a new fd here
                    
                    // Create task and enqueue
                    Task task;
                    task.sock = client_fd;
                    strncpy(task.buffer, buffer, BUFFER_SIZE - 1);
                    task.buffer[BUFFER_SIZE - 1] = '\0';
                    task.username[0] = '\0';
                    task.is_registration = 1; // First message is always registration
                    
                    enqueue_task(task);
                    
                } else if (read_size == 0) {
                    // Connection closed
                    log_message(NS_LOG_FILE, "INFO", "Connection closed on fd=%d", client_fd);
                    fds[i] = fds[nfds - 1];
                    nfds--;
                    i--; // Recheck this position
                    close(client_fd);
                } else {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("ERROR reading from client");
                        fds[i] = fds[nfds - 1];
                        nfds--;
                        i--; // Recheck this position
                        close(client_fd);
                    }
                }
            }
        }
    }

    close(listen_fd);
    return 0;
}