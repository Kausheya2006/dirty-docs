#ifndef NS_UTILS_H
#define NS_UTILS_H

#include "../common/utils.h"
#include <time.h>

#define MAX_FILENAME 256
#define MAX_USERS 50
#define MAX_SS 10
#define MAX_CLIENTS 100  // Increased for poll array size
#define REPLICATION_FACTOR 2  // Number of copies (primary + replicas)

// --- Access Control ---
typedef struct {
    char* read_users[MAX_USERS];
    char* write_users[MAX_USERS];
    int read_count;
    int write_count;
} Users;

typedef enum {
    PERM_NONE,
    PERM_READ,
    PERM_WRITE
} PermissionLevel;

// --- FileTrie Node ---
typedef struct FileNode {
    char* owner;
    char* ss_ids[MAX_SS]; // Array of Storage Server IDs that have this file (replicas)
    int ss_count;         // Number of replicas
    long size;
    long word_count;      // Word count (updated from SS)
    long char_count;      // Character count (updated from SS)
    time_t creation_time;
    time_t last_modified;
    time_t last_access;   // Last access time (updated from SS)
    Users acl;
    struct FileNode* children[128]; // For all ASCII chars
    int is_end_of_word; // 1 if this node marks the end of a filename
    int is_folder; // 1 if this node is a folder, 0 if it's a file
    int is_in_trash; // 1 if file is in trash, 0 otherwise
} FileNode;

// --- Storage Server Info ---
typedef struct {
    char id[50];
    char ip[50];
    int client_port; // Port for clients to connect (for READ/WRITE)
    int nm_port;     // Port for NM to connect (for CREATE/DELETE)
    int is_active;
    time_t last_heartbeat; // For failure detection
} StorageServer;

// --- Connected Client Info ---
typedef struct {
    char username[100];
    int socket_fd;
    int is_active;
} ClientSession;

// --- Trie Function Prototypes ---
FileNode* create_file_node();
void insert_file(FileNode* root, const char* filename, const char* owner, const char* ss_id);
void insert_file_with_replicas(FileNode* root, const char* filename, const char* owner, char** ss_ids, int ss_count);
FileNode* find_file(FileNode* root, const char* filename);
int delete_file(FileNode* root, const char* filename, int depth);
void traverse_trie_recursive(FileNode* node, const char* username, int list_all, int show_details, char* output_buffer, char* current_prefix);
void list_files(FileNode* root, const char* username, int list_all, int show_details, char* output_buffer);

// --- Folder Function Prototypes ---
void insert_folder(FileNode* root, const char* foldername, const char* owner, const char* ss_id);
FileNode* find_folder(FileNode* root, const char* foldername);
int move_file_to_folder(FileNode* root, const char* filename, const char* foldername);
int move_file(FileNode* root, const char* src_path, const char* dest_folder_path);
void list_folder_contents(FileNode* root, const char* foldername, const char* username, char* output_buffer);

// --- Trash Function Prototypes ---
FileNode* find_file_any_status(FileNode* root, const char* filename);
void list_trash_recursive(FileNode* node, const char* username, char* output_buffer, char* current_prefix);
void list_trash(FileNode* root, const char* username, char* output_buffer);
char* get_base_filename(const char* path);

// --- Permission Check ---
PermissionLevel check_permission(FileNode* node, const char* username);
// (You will also need functions for 'traverse_files' for VIEW)

// --- Persistence Functions ---
void save_trie_to_file(FileNode* root, const char* filepath);
int load_trie_from_file(FileNode** root, const char* filepath);

#endif