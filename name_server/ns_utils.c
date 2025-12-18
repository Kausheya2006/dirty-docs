#include "ns_utils.h"
#include <string.h>

FileNode* create_file_node() {
    FileNode* node = (FileNode*)calloc(1, sizeof(FileNode)); // calloc initializes to zero
    if (node == NULL) {
        die("calloc failed for FileNode");
    }
    node->is_end_of_word = 0;
    node->is_folder = 0; // Default is file, not folder
    node->is_in_trash = 0; // Not in trash by default
    node->acl.read_count = 0;
    node->acl.write_count = 0;
    node->ss_count = 0; // No replicas initially
    for (int i = 0; i < MAX_SS; i++) {
        node->ss_ids[i] = NULL;
    }
    return node;
}

void insert_file(FileNode* root, const char* filename, const char* owner, const char* ss_id) {
    FileNode* current = root;
    for (int i = 0; filename[i] != '\0'; i++) {
        int index = (int)filename[i];
        if (current->children[index] == NULL) {
            current->children[index] = create_file_node();
        }
        current = current->children[index];
    }
    current->is_end_of_word = 1;
    current->owner = strdup(owner);
    // Store single SS ID (backward compatibility)
    current->ss_ids[0] = strdup(ss_id);
    current->ss_count = 1;
    current->creation_time = time(NULL);
    current->last_modified = time(NULL);
    // Add owner to write access list by default
    current->acl.write_users[current->acl.write_count++] = strdup(owner);
}

// New function to insert file with multiple replicas
void insert_file_with_replicas(FileNode* root, const char* filename, const char* owner, char** ss_ids, int ss_count) {
    FileNode* current = root;
    for (int i = 0; filename[i] != '\0'; i++) {
        int index = (int)filename[i];
        if (current->children[index] == NULL) {
            current->children[index] = create_file_node();
        }
        current = current->children[index];
    }
    current->is_end_of_word = 1;
    current->owner = strdup(owner);
    // Store all replica SS IDs
    current->ss_count = ss_count;
    for (int i = 0; i < ss_count && i < MAX_SS; i++) {
        current->ss_ids[i] = strdup(ss_ids[i]);
    }
    current->creation_time = time(NULL);
    current->last_modified = time(NULL);
    // Add owner to write access list by default
    current->acl.write_users[current->acl.write_count++] = strdup(owner);
}

FileNode* find_file(FileNode* root, const char* filename) {
    FileNode* current = root;
    for (int i = 0; filename[i] != '\0'; i++) {
        int index = (int)filename[i];
        if (current->children[index] == NULL) {
            return NULL; // Not found
        }
        current = current->children[index];
    }
    
    // Only return files that are NOT in trash
    if (current != NULL && current->is_end_of_word && !current->is_in_trash) {
        return current;
    }
    return NULL;
}

// Find file regardless of trash status (for internal commands like RESTORE)
FileNode* find_file_any_status(FileNode* root, const char* filename) {
    FileNode* current = root;
    for (int i = 0; filename[i] != '\0'; i++) {
        int index = (int)filename[i];
        if (current->children[index] == NULL) {
            return NULL; // Not found
        }
        current = current->children[index];
    }
    
    // This version does NOT check is_in_trash
    if (current != NULL && current->is_end_of_word) {
        return current;
    }
    return NULL;
}

// Note: A full 'delete_file' is complex as it needs to free nodes.
// For now, you can just find the node and set is_end_of_word = 0
// A better implementation would garbage collect unused nodes.
int delete_file(FileNode* root, const char* filename, int depth) {
    // This is a simplified "lazy delete". 
    // A proper recursive delete is more complex.
    FileNode* node = find_file(root, filename);
    if (node) {
        node->is_end_of_word = 0;
        // You should also free the strdup'd strings
        free(node->owner);
        for (int i = 0; i < node->ss_count && i < MAX_SS; i++) {
            if (node->ss_ids[i]) free(node->ss_ids[i]);
        }
        // ... and clear ACLs
        return 1; // Success
    }
    return 0; // Failure
}

PermissionLevel check_permission(FileNode* node, const char* username) {
    if (node == NULL || username == NULL) {
        return PERM_NONE;
    }

    // 1. Check if they are the owner
    if (strcmp(node->owner, username) == 0) {
        return PERM_WRITE; // Owner has full R/W access
    }

    // 2. Check the write list
    for (int i = 0; i < node->acl.write_count; i++) {
        if (strcmp(node->acl.write_users[i], username) == 0) {
            return PERM_WRITE;
        }
    }

    // 3. Check the read list
    for (int i = 0; i < node->acl.read_count; i++) {
        if (strcmp(node->acl.read_users[i], username) == 0) {
            return PERM_READ;
        }
    }

    return PERM_NONE;
}

void traverse_trie_recursive(FileNode* node, const char* username, int list_all, int show_details, char* output_buffer, char* current_prefix) {
    if (node == NULL) {
        return;
    }

    // If this node marks the end of a filename, check permissions
    if (node->is_end_of_word && !node->is_in_trash) {
        // A file exists at this prefix. Check if user can see it.
        if (list_all || check_permission(node, username) >= PERM_READ) {
            // Simple listing: just filename
            strcat(output_buffer, current_prefix);
            // Add trailing slash for folders
            if (node->is_folder) {
                strcat(output_buffer, "/");
            }
            strcat(output_buffer, "\n");
        }
    }

    // Recurse for all children
    for (int i = 0; i < 128; i++) {
        if (node->children[i] != NULL) {
            // Append current char to prefix
            int len = strlen(current_prefix);
            current_prefix[len] = (char)i;
            current_prefix[len + 1] = '\0';
            
            traverse_trie_recursive(node->children[i], username, list_all, show_details, output_buffer, current_prefix);
            
            // Backtrack: remove char from prefix
            current_prefix[len] = '\0';
        }
    }
}

void list_files(FileNode* root, const char* username, int list_all, int show_details, char* output_buffer) {
    char prefix_buffer[MAX_FILENAME * 2];
    bzero(prefix_buffer, sizeof(prefix_buffer));
    output_buffer[0] = '\0'; // Clear the output buffer
    
    // traverse_trie_recursive will fill the output_buffer
    traverse_trie_recursive(root, username, list_all, show_details, output_buffer, prefix_buffer);
}

// --- Trash-specific Functions ---

// Recursive helper for list_trash
void list_trash_recursive(FileNode* node, const char* username, char* output_buffer, char* current_prefix) {
    if (node == NULL) return;

    // If this is a file/folder and it IS in the trash
    if (node->is_end_of_word && node->is_in_trash) {
        // Only list items owned by the user
        if (strcmp(node->owner, username) == 0) {
            strcat(output_buffer, current_prefix);
            if (node->is_folder) {
                strcat(output_buffer, "/");
            }
            strcat(output_buffer, "\n");
        }
    }

    // Recurse
    for (int i = 0; i < 128; i++) {
        if (node->children[i] != NULL) {
            int len = strlen(current_prefix);
            current_prefix[len] = (char)i;
            current_prefix[len + 1] = '\0';
            list_trash_recursive(node->children[i], username, output_buffer, current_prefix);
            current_prefix[len] = '\0'; // Backtrack
        }
    }
}

// Public wrapper for list_trash
void list_trash(FileNode* root, const char* username, char* output_buffer) {
    char prefix_buffer[MAX_FILENAME * 2];
    bzero(prefix_buffer, sizeof(prefix_buffer));
    output_buffer[0] = '\0';
    
    list_trash_recursive(root, username, output_buffer, prefix_buffer);
}

// Helper to extract base filename from path
char* get_base_filename(const char* path) {
    const char* last_slash = strrchr(path, '/');
    if (last_slash == NULL) {
        return (char*)path; // No slash, it's already a base filename
    }
    return (char*)(last_slash + 1); // Return the part after the slash
}

// --- Folder-specific Functions ---

void insert_folder(FileNode* root, const char* foldername, const char* owner, const char* ss_id) {
    FileNode* current = root;
    for (int i = 0; foldername[i] != '\0'; i++) {
        int index = (int)foldername[i];
        if (current->children[index] == NULL) {
            current->children[index] = create_file_node();
        }
        current = current->children[index];
    }
    current->is_end_of_word = 1;
    current->is_folder = 1; // Mark as folder
    current->owner = strdup(owner);
    current->ss_ids[0] = strdup(ss_id);
    current->ss_count = 1;
    current->creation_time = time(NULL);
    current->last_modified = time(NULL);
    // Add owner to write access list by default
    current->acl.write_users[current->acl.write_count++] = strdup(owner);
}

FileNode* find_folder(FileNode* root, const char* foldername) {
    FileNode* node = find_file(root, foldername);
    if (node != NULL && node->is_folder) {
        return node;
    }
    return NULL;
}

// Move a file to a folder by creating new path (folder/filename)
int move_file_to_folder(FileNode* root, const char* filename, const char* foldername) {
    // Find the source file
    FileNode* file_node = find_file(root, filename);
    if (file_node == NULL || file_node->is_folder) {
        return 0; // File not found or it's a folder
    }
    
    // Find the destination folder
    FileNode* folder_node = find_folder(root, foldername);
    if (folder_node == NULL) {
        return 0; // Folder not found
    }
    
    // Create new path: foldername/filename
    char new_path[MAX_FILENAME * 2];
    snprintf(new_path, sizeof(new_path), "%s/%s", foldername, filename);
    
    // Copy file metadata to new location (use first SS ID)
    if (file_node->ss_count > 1) {
        char* ss_ids_copy[MAX_SS];
        for (int i = 0; i < file_node->ss_count && i < MAX_SS; i++) {
            ss_ids_copy[i] = file_node->ss_ids[i];
        }
        insert_file_with_replicas(root, new_path, file_node->owner, ss_ids_copy, file_node->ss_count);
    } else {
        insert_file(root, new_path, file_node->owner, file_node->ss_ids[0]);
    }
    FileNode* new_node = find_file(root, new_path);
    if (new_node) {
        new_node->size = file_node->size;
        new_node->creation_time = file_node->creation_time;
        new_node->last_modified = file_node->last_modified;
        // Copy ACLs
        for (int i = 0; i < file_node->acl.read_count; i++) {
            new_node->acl.read_users[new_node->acl.read_count++] = strdup(file_node->acl.read_users[i]);
        }
        for (int i = 0; i < file_node->acl.write_count; i++) {
            // Skip owner as it's already added
            if (strcmp(file_node->acl.write_users[i], file_node->owner) != 0) {
                new_node->acl.write_users[new_node->acl.write_count++] = strdup(file_node->acl.write_users[i]);
            }
        }
    }
    
    // Delete old file entry
    delete_file(root, filename, 0);
    
    return 1; // Success
}

// New move_file function that handles both folder and root destinations
int move_file(FileNode* root, const char* src_path, const char* dest_folder_path) {
    // Find the source file/folder
    FileNode* file_node = find_file(root, src_path);
    if (file_node == NULL) {
        return 0; // File not found
    }
    
    char new_path[MAX_FILENAME * 2];
    char* base_filename = get_base_filename(src_path);

    // Check if moving to root (destination is ".")
    if (strcmp(dest_folder_path, ".") == 0) {
        // New path is just the base filename
        snprintf(new_path, sizeof(new_path), "%s", base_filename);
    } else {
        // Find the destination folder
        FileNode* folder_node = find_folder(root, dest_folder_path);
        if (folder_node == NULL) {
            return 0; // Folder not found
        }
        // Create new path: foldername/filename
        snprintf(new_path, sizeof(new_path), "%s/%s", dest_folder_path, base_filename);
    }

    // Check if destination already exists
    if (find_file(root, new_path) != NULL) {
        return 0; // Destination path already exists
    }
    
    // Copy file metadata to new location
    if (file_node->is_folder) {
        insert_folder(root, new_path, file_node->owner, file_node->ss_ids[0]);
    } else {
        if (file_node->ss_count > 1) {
            insert_file_with_replicas(root, new_path, file_node->owner, file_node->ss_ids, file_node->ss_count);
        } else {
            insert_file(root, new_path, file_node->owner, file_node->ss_ids[0]);
        }
    }
    
    // Copy metadata
    FileNode* new_node = find_file(root, new_path);
    if (new_node) {
        new_node->size = file_node->size;
        new_node->creation_time = file_node->creation_time;
        new_node->last_modified = time(NULL); // Update modified time
        new_node->is_in_trash = file_node->is_in_trash; // Preserve trash status
        
        // Copy ACLs
        for (int i = 0; i < file_node->acl.read_count; i++) {
            new_node->acl.read_users[new_node->acl.read_count++] = strdup(file_node->acl.read_users[i]);
        }
        for (int i = 0; i < file_node->acl.write_count; i++) {
            if (strcmp(file_node->acl.write_users[i], file_node->owner) != 0) {
                new_node->acl.write_users[new_node->acl.write_count++] = strdup(file_node->acl.write_users[i]);
            }
        }
    }
    
    // Delete old file entry
    delete_file(root, src_path, 0);
    
    return 1; // Success
}

// Helper function to traverse and collect folder contents
static void traverse_for_folder(FileNode* node, char* prefix, const char* folder_prefix, 
                                 int prefix_len, const char* username, char* output_buffer) {
    if (node == NULL) return;
    
    if (node->is_end_of_word) {
        // Check if this path starts with our folder prefix
        if (strncmp(prefix, folder_prefix, prefix_len) == 0) {
            // Extract just the filename part after the folder
            const char* filename_part = prefix + prefix_len;
            // Only show direct children (not nested folders)
            if (strchr(filename_part, '/') == NULL && strlen(filename_part) > 0) {
                if (check_permission(node, username) >= PERM_READ) {
                    strcat(output_buffer, filename_part);
                    if (node->is_folder) {
                        strcat(output_buffer, "/");
                    }
                    strcat(output_buffer, "\n");
                }
            }
        }
    }
    
    // Recurse
    for (int i = 0; i < 128; i++) {
        if (node->children[i] != NULL) {
            int len = strlen(prefix);
            prefix[len] = (char)i;
            prefix[len + 1] = '\0';
            traverse_for_folder(node->children[i], prefix, folder_prefix, prefix_len, username, output_buffer);
            prefix[len] = '\0';
        }
    }
}

// List all files in a specific folder
void list_folder_contents(FileNode* root, const char* foldername, const char* username, char* output_buffer) {
    // Find the folder
    FileNode* folder_node = find_folder(root, foldername);
    if (folder_node == NULL) {
        strcpy(output_buffer, "ERR_FOLDER_NOT_FOUND\n");
        return;
    }
    
    // Check if user has permission to view folder
    if (check_permission(folder_node, username) < PERM_READ) {
        strcpy(output_buffer, "ERR_PERMISSION_DENIED\n");
        return;
    }
    
    // Build the folder prefix for searching
    char folder_prefix[MAX_FILENAME * 2];
    snprintf(folder_prefix, sizeof(folder_prefix), "%s/", foldername);
    int prefix_len = strlen(folder_prefix);
    
    output_buffer[0] = '\0';
    
    // Traverse the entire trie and find entries that start with folder_prefix
    char current_prefix[MAX_FILENAME * 2];
    bzero(current_prefix, sizeof(current_prefix));
    
    traverse_for_folder(root, current_prefix, folder_prefix, prefix_len, username, output_buffer);
    
    if (strlen(output_buffer) == 0) {
        strcpy(output_buffer, "Folder is empty.\n");
    }
}

// ========== PERSISTENCE FUNCTIONS ==========

// Helper function to write a string to file (with length prefix)
static void write_string(FILE* fp, const char* str) {
    if (str == NULL) {
        int len = -1;
        fwrite(&len, sizeof(int), 1, fp);
    } else {
        int len = strlen(str);
        fwrite(&len, sizeof(int), 1, fp);
        fwrite(str, sizeof(char), len, fp);
    }
}

// Helper function to read a string from file (with length prefix)
static char* read_string(FILE* fp) {
    int len;
    if (fread(&len, sizeof(int), 1, fp) != 1) return NULL;
    if (len == -1) return NULL;
    if (len < 0 || len > 10000) return NULL; // Sanity check
    
    char* str = (char*)malloc(len + 1);
    if (fread(str, sizeof(char), len, fp) != (size_t)len) {
        free(str);
        return NULL;
    }
    str[len] = '\0';
    return str;
}

// Helper to serialize a FileNode recursively
static void serialize_node(FileNode* node, FILE* fp, char* path, int depth) {
    if (node == NULL) return;
    
    // If this is an end node (file or folder), save it
    if (node->is_end_of_word) {
        // Write marker
        char marker = 'F';
        fwrite(&marker, sizeof(char), 1, fp);
        
        // Write path
        write_string(fp, path);
        
        // Write node data
        write_string(fp, node->owner);
        
        // Write replica count and all SS IDs
        fwrite(&node->ss_count, sizeof(int), 1, fp);
        for (int i = 0; i < node->ss_count && i < MAX_SS; i++) {
            write_string(fp, node->ss_ids[i]);
        }
        
        fwrite(&node->size, sizeof(long), 1, fp);
        fwrite(&node->creation_time, sizeof(time_t), 1, fp);
        fwrite(&node->last_modified, sizeof(time_t), 1, fp);
        fwrite(&node->is_folder, sizeof(int), 1, fp);
        fwrite(&node->is_in_trash, sizeof(int), 1, fp);
        
        // Write ACL
        fwrite(&node->acl.read_count, sizeof(int), 1, fp);
        for (int i = 0; i < node->acl.read_count; i++) {
            write_string(fp, node->acl.read_users[i]);
        }
        fwrite(&node->acl.write_count, sizeof(int), 1, fp);
        for (int i = 0; i < node->acl.write_count; i++) {
            write_string(fp, node->acl.write_users[i]);
        }
    }
    
    // Recursively serialize children
    for (int i = 0; i < 128; i++) {
        if (node->children[i] != NULL) {
            char new_path[MAX_FILENAME * 4];
            snprintf(new_path, sizeof(new_path), "%s%c", path, (char)i);
            serialize_node(node->children[i], fp, new_path, depth + 1);
        }
    }
}

// Save the entire trie to a file
void save_trie_to_file(FileNode* root, const char* filepath) {
    FILE* fp = fopen(filepath, "wb");
    if (fp == NULL) {
        printf("[NM] WARNING: Could not open %s for writing\n", filepath);
        return;
    }
    
    // Write a magic header (updated version for multi-replica support)
    char magic[] = "NMTRIE02";
    fwrite(magic, sizeof(char), 8, fp);
    
    // Serialize the trie
    char path[MAX_FILENAME * 4] = "";
    serialize_node(root, fp, path, 0);
    
    // Write end marker
    char end_marker = 'E';
    fwrite(&end_marker, sizeof(char), 1, fp);
    
    fclose(fp);
    printf("[NM] Trie saved to %s\n", filepath);
}

// Load the trie from a file
int load_trie_from_file(FileNode** root, const char* filepath) {
    FILE* fp = fopen(filepath, "rb");
    if (fp == NULL) {
        printf("[NM] No persistence file found at %s, starting with empty trie\n", filepath);
        return 0; // Not an error, just no saved data
    }
    
    // Read and verify magic header
    char magic[9];
    if (fread(magic, sizeof(char), 8, fp) != 8) {
        printf("[NM] ERROR: Invalid persistence file format\n");
        fclose(fp);
        return -1;
    }
    magic[8] = '\0';
    // Accept both old (NMTRIE01) and new (NMTRIE02) formats
    if (strcmp(magic, "NMTRIE02") != 0 && strcmp(magic, "NMTRIE01") != 0) {
        printf("[NM] ERROR: Invalid magic header '%s' in persistence file (expected NMTRIE02)\n", magic);
        printf("[NM] Deleting corrupted file and starting fresh\n");
        fclose(fp);
        remove(filepath);
        return 0; // Start with empty trie
    }
    
    // Check if this is the old format
    int is_old_format = (strcmp(magic, "NMTRIE01") == 0);
    if (is_old_format) {
        printf("[NM] WARNING: Old persistence format detected (NMTRIE01)\n");
        printf("[NM] This format is incompatible with replication. Starting with empty trie.\n");
        fclose(fp);
        remove(filepath);
        return 0;
    }
    
    // Create new root if needed
    if (*root == NULL) {
        *root = create_file_node();
    }
    
    // Read entries until end marker
    while (1) {
        char marker;
        if (fread(&marker, sizeof(char), 1, fp) != 1) break;
        
        if (marker == 'E') {
            // End of file
            break;
        } else if (marker == 'F') {
            // File/folder entry
            char* path = read_string(fp);
            if (path == NULL) break;
            
            char* owner = read_string(fp);
            
            // Read replica count and SS IDs
            int ss_count;
            fread(&ss_count, sizeof(int), 1, fp);
            char* ss_ids[MAX_SS];
            for (int i = 0; i < ss_count && i < MAX_SS; i++) {
                ss_ids[i] = read_string(fp);
            }
            
            long size;
            time_t creation_time, last_modified;
            int is_folder;
            
            fread(&size, sizeof(long), 1, fp);
            fread(&creation_time, sizeof(time_t), 1, fp);
            fread(&last_modified, sizeof(time_t), 1, fp);
            fread(&is_folder, sizeof(int), 1, fp);
            int is_in_trash = 0;
            fread(&is_in_trash, sizeof(int), 1, fp);
            
            // Insert into trie
            if (is_folder) {
                insert_folder(*root, path, owner, ss_ids[0]); // Folders use first SS only
            } else {
                if (ss_count > 1) {
                    insert_file_with_replicas(*root, path, owner, ss_ids, ss_count);
                } else {
                    insert_file(*root, path, owner, ss_ids[0]);
                }
            }
            
            // Restore additional metadata
            FileNode* node = find_file_any_status(*root, path);
            if (node != NULL) {
                node->size = size;
                node->creation_time = creation_time;
                node->last_modified = last_modified;
                node->is_in_trash = is_in_trash;
                
                // Read ACL
                int read_count, write_count;
                fread(&read_count, sizeof(int), 1, fp);
                node->acl.read_count = read_count;
                for (int i = 0; i < read_count && i < MAX_USERS; i++) {
                    node->acl.read_users[i] = read_string(fp);
                }
                fread(&write_count, sizeof(int), 1, fp);
                node->acl.write_count = write_count;
                for (int i = 0; i < write_count && i < MAX_USERS; i++) {
                    node->acl.write_users[i] = read_string(fp);
                }
            }
            
            free(path);
            // owner and ss_ids are copied by insert_file/insert_folder
            if (owner) free(owner);
            for (int i = 0; i < ss_count && i < MAX_SS; i++) {
                if (ss_ids[i]) free(ss_ids[i]);
            }
        }
    }
    
    fclose(fp);
    printf("[NM] Trie loaded from %s\n", filepath);
    return 1;
}