#ifndef SS_UTILS_H
#define SS_UTILS_H

#include "../common/utils.h"
#include "../name_server/ns_utils.h"

// Struct to manage sentence-level locks for a file
#define MAX_LOCKED_SENTENCES 100
typedef struct {
    int sentence_num;           // 1-indexed sentence number
    char sentence_content[2048]; // Content of the locked sentence (for verification)
} SentenceLock;

typedef struct {
    char filename[MAX_FILENAME];
    SentenceLock locked_sentences[MAX_LOCKED_SENTENCES];
    int locked_count; // Number of currently locked sentences
    pthread_mutex_t mutex; // Protects this struct
} FileLock;

// A global list of locks (one per file *currently being edited*)
#define MAX_FILE_LOCKS 100
extern FileLock file_locks[MAX_FILE_LOCKS];
extern int file_lock_count;
extern pthread_mutex_t file_lock_list_mutex; // Protects the list itself

// Function prototypes
FileLock* get_or_create_file_lock(const char* filename);
int lock_sentence(const char* filename, int sentence_num, const char* sentence_content);
void unlock_sentence(const char* filename, int sentence_num);
int find_sentence_by_content(const char* filepath, const char* locked_content, int original_num);
int is_file_locked(const char* filename);  // NEW: Check if file has any active locks

void handle_read(int sock, const char* filepath);
void handle_stream(int sock, const char* filepath);
void handle_write(int sock, const char* filepath, int sentence_num);
void handle_undo(int sock, const char* filepath); 

// --- Checkpoint handlers ---
void handle_checkpoint(int sock, const char* filepath, const char* tag);
void handle_viewcheckpoint(int sock, const char* filepath, const char* tag);
void handle_listcheckpoints(int sock, const char* filepath);
void handle_revert_to_checkpoint(int sock, const char* filepath, const char* tag);
int read_sentences_from_file(const char* filepath, char sentences[][2048], int max_sentences);

#endif