#include "ss_utils.h"
#include <ctype.h>
#include "../common/config.h"
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>
// Global lock list
FileLock file_locks[MAX_FILE_LOCKS];
int file_lock_count = 0;
pthread_mutex_t file_lock_list_mutex = PTHREAD_MUTEX_INITIALIZER;

// Finds a lock struct for a file, or creates one
FileLock* get_or_create_file_lock(const char* filename) {
    pthread_mutex_lock(&file_lock_list_mutex);
    
    // 1. Try to find existing lock
    for (int i = 0; i < file_lock_count; i++) {
        if (strcmp(file_locks[i].filename, filename) == 0) {
            pthread_mutex_unlock(&file_lock_list_mutex);
            return &file_locks[i];
        }
    }
    
    // 2. Not found, create a new one
    if (file_lock_count >= MAX_FILE_LOCKS) {
        pthread_mutex_unlock(&file_lock_list_mutex);
        return NULL; // No space for new locks
    }
    
    FileLock* new_lock = &file_locks[file_lock_count];
    strcpy(new_lock->filename, filename);
    new_lock->locked_count = 0;
    for (int i = 0; i < MAX_LOCKED_SENTENCES; i++) {
        new_lock->locked_sentences[i].sentence_num = -1;
        new_lock->locked_sentences[i].sentence_content[0] = '\0';
    }
    pthread_mutex_init(&new_lock->mutex, NULL);
    
    file_lock_count++;
    
    pthread_mutex_unlock(&file_lock_list_mutex);
    return new_lock;
}

// Tries to lock a sentence for a file
// Returns 1 on success, 0 on failure (already locked)
int lock_sentence(const char* filename, int sentence_num, const char* sentence_content) {
    FileLock* lock = get_or_create_file_lock(filename);
    if (lock == NULL) return 0; // Failed to get lock struct

    pthread_mutex_lock(&lock->mutex);
    
    // Check if this specific sentence is already locked
    for (int i = 0; i < lock->locked_count; i++) {
        if (lock->locked_sentences[i].sentence_num == sentence_num) {
            // This sentence is already locked
            pthread_mutex_unlock(&lock->mutex);
            return 0; // Failure
        }
    }
    
    // Check if we have space for another lock
    if (lock->locked_count >= MAX_LOCKED_SENTENCES) {
        pthread_mutex_unlock(&lock->mutex);
        return 0; // No space for more locks
    }
    
    // Lock this sentence and store its content
    lock->locked_sentences[lock->locked_count].sentence_num = sentence_num;
    strncpy(lock->locked_sentences[lock->locked_count].sentence_content, sentence_content, 2047);
    lock->locked_sentences[lock->locked_count].sentence_content[2047] = '\0';
    lock->locked_count++;
    
    pthread_mutex_unlock(&lock->mutex);
    return 1; // Success
}

// Unlocks a sentence
void unlock_sentence(const char* filename, int sentence_num) {
    FileLock* lock = get_or_create_file_lock(filename); // Should exist
    if (lock == NULL) return;

    pthread_mutex_lock(&lock->mutex);
    
    // Find and remove this sentence from the locked list
    for (int i = 0; i < lock->locked_count; i++) {
        if (lock->locked_sentences[i].sentence_num == sentence_num) {
            // Found it - shift remaining entries left
            for (int j = i; j < lock->locked_count - 1; j++) {
                lock->locked_sentences[j] = lock->locked_sentences[j + 1];
            }
            lock->locked_sentences[lock->locked_count - 1].sentence_num = -1;
            lock->locked_sentences[lock->locked_count - 1].sentence_content[0] = '\0';
            lock->locked_count--;
            break;
        }
    }
    
    pthread_mutex_unlock(&lock->mutex);
}

// Check if a file has any active locks
int is_file_locked(const char* filename) {
    pthread_mutex_lock(&file_lock_list_mutex);
    
    printf("[SS] Checking locks for file: '%s'\n", filename);
    printf("[SS] Current lock list has %d files:\n", file_lock_count);
    for (int i = 0; i < file_lock_count; i++) {
        printf("[SS]   File %d: '%s' (locked_count: %d)\n", i, file_locks[i].filename, file_locks[i].locked_count);
    }
    
    // Search for this file in the lock list
    for (int i = 0; i < file_lock_count; i++) {
        if (strcmp(file_locks[i].filename, filename) == 0) {
            pthread_mutex_lock(&file_locks[i].mutex);
            int locked = (file_locks[i].locked_count > 0);
            pthread_mutex_unlock(&file_locks[i].mutex);
            pthread_mutex_unlock(&file_lock_list_mutex);
            printf("[SS] Lock check result: %s (locked_count: %d)\n", locked ? "LOCKED" : "UNLOCKED", file_locks[i].locked_count);
            return locked;
        }
    }
    
    pthread_mutex_unlock(&file_lock_list_mutex);
    printf("[SS] Lock check result: UNLOCKED (file not in lock list)\n");
    return 0; // File not found in lock list, so not locked
}

// Find a sentence by its content (used when sentence numbers shift)
int find_sentence_by_content(const char* filepath, const char* locked_content, int original_num) {
    char (*sentences)[2048] = malloc(100 * sizeof(char[2048]));
    if (!sentences) return -1;
    
    int sentence_count = read_sentences_from_file(filepath, sentences, 100);
    
    printf("[SS] find_sentence_by_content: Searching for locked content (len=%zu)\n", strlen(locked_content));
    printf("[SS]   Locked content: '%s'\n", locked_content);
    printf("[SS]   Original position: %d, Current sentence count: %d\n", original_num, sentence_count);
    
    // First try the original position
    if (original_num > 0 && original_num <= sentence_count) {
        printf("[SS]   Checking original position %d: '%s'\n", original_num, sentences[original_num - 1]);
        if (strcmp(sentences[original_num - 1], locked_content) == 0) {
            printf("[SS]   ✓ Found at original position %d\n", original_num);
            free(sentences);
            return original_num;
        }
    }
    
    // Search for the sentence by content
    for (int i = 0; i < sentence_count; i++) {
        printf("[SS]   Checking position %d: '%s' (len=%zu)\n", i + 1, sentences[i], strlen(sentences[i]));
        if (strcmp(sentences[i], locked_content) == 0) {
            printf("[SS]   ✓ Found at position %d (moved from %d)\n", i + 1, original_num);
            free(sentences);
            return i + 1; // Return 1-indexed
        }
    }
    
    printf("[SS]   ✗ Sentence not found in file!\n");
    free(sentences);
    return -1; // Not found
}

// --- File I/O Handlers ---

void handle_read(int sock, const char* filepath) {
    FILE* f = fopen(filepath, "r");
    if (f == NULL) {
        write(sock, "ERR_SS_FILE_NOT_FOUND\n", 22);
        return;
    }
    
    char buffer[BUFFER_SIZE];
    size_t n;
    while ((n = fread(buffer, 1, BUFFER_SIZE, f)) > 0) {
        if (write(sock, buffer, n) < 0) {
            break; // Client disconnected
        }
    }
    fclose(f);
}

// In storage_server/ss_utils.c
void handle_stream(int sock, const char* filepath) {
    FILE* f = fopen(filepath, "r");
    if (f == NULL) {
        write(sock, "ERR_SS_FILE_NOT_FOUND\n", 22);
        return;
    }

    char word[256];
    int ch, i = 0;
    while ((ch = fgetc(f)) != EOF) {
        if (ch == '.' || ch == '!' || ch == '?') {
            // Punctuation. Attach to word and send.
            word[i++] = ch;
            word[i] = '\0';
            write(sock, word, strlen(word));
            usleep(100000);
            i = 0;
        } else if (isspace(ch)) {
            // Space. Send previous word (if any).
            if (i > 0) {
                word[i] = '\0';
                write(sock, word, strlen(word));
                usleep(100000);
                i = 0;
            }
            // *Always* send the space.
            write(sock, &ch, 1); // Send the actual space/newline char
            usleep(100000);
        } else {
            // Regular char
            word[i++] = ch;
            if (i >= 255) { // Word too long
                word[i] = '\0';
                write(sock, word, strlen(word));
                usleep(100000);
                i = 0;
            }
        }
    }
    if (i > 0) { // Send last word
        word[i] = '\0';
        write(sock, word, strlen(word));
        usleep(100000);
    }
    fclose(f);
}
// Structure to store write operations
typedef struct {
    int word_index;
    char content[1024];
} WriteOperation;

// Helper function to split a sentence into words
int split_sentence_into_words(const char* sentence, char words[][256], int max_words) {
    int word_count = 0;
    int i = 0, j = 0;
    int len = strlen(sentence);
    
    while (i < len && word_count < max_words) {
        // Skip leading spaces
        while (i < len && isspace(sentence[i])) i++;
        if (i >= len) break;
        
        // Read word
        j = 0;
        while (i < len && !isspace(sentence[i])) {
            words[word_count][j++] = sentence[i++];
        }
        words[word_count][j] = '\0';
        word_count++;
    }
    
    return word_count;
}

// Helper function to read all sentences from a file
int read_sentences_from_file(const char* filepath, char sentences[][2048], int max_sentences) {
    FILE* f = fopen(filepath, "r");
    if (!f) return 0;
    
    int sentence_count = 0;
    int ch, idx = 0;
    
    // Skip leading whitespace before first sentence only
    while ((ch = fgetc(f)) != EOF && isspace(ch));
    if (ch != EOF) ungetc(ch, f);
    
    while ((ch = fgetc(f)) != EOF && sentence_count < max_sentences) {
        // Don't skip spaces - they're part of the sentence content
        
        // Check for buffer overflow
        if (idx >= 2047) {
            fprintf(stderr, "[SS] Warning: Sentence too long, truncating at 2048 chars\n");
            sentences[sentence_count][idx] = '\0';
            // Skip to next delimiter or EOF
            while ((ch = fgetc(f)) != EOF) {
                if (ch == '.' || ch == '!' || ch == '?') {
                    sentence_count++;
                    idx = 0;
                    break;
                }
            }
            continue;
        }
        
        sentences[sentence_count][idx++] = ch;
        
        if (ch == '.' || ch == '!' || ch == '?') {
            sentences[sentence_count][idx] = '\0';
            sentence_count++;
            idx = 0;
        }
    }
    
    // Handle any remaining text (sentence without delimiter)
    // This is still part of the LAST sentence (or sentence 1 if file is empty)
    // It doesn't create a new sentence - only delimiters create sentences
    // But we need to include this content when reading the last sentence
    if (idx > 0) {
        // If we have previous sentences, this belongs to a continuation
        // If no previous sentences, this is sentence 1 (incomplete)
        sentences[sentence_count][idx] = '\0';
        sentence_count++;  // Count it so it can be edited, but it's still the same sentence number
        printf("[SS] Found incomplete sentence (no delimiter): '%s'\n", sentences[sentence_count-1]);
    }
    
    fclose(f);
    return sentence_count;
}

void handle_write(int sock, const char* filepath, int sentence_num) {
    // 1. FIRST validate sentence range BEFORE locking
    // Read the file to check sentence count
    char (*validation_sentences)[2048] = malloc(100 * sizeof(char[2048]));
    if (!validation_sentences) {
        write(sock, "ERR_MEMORY\n", 11);
        return;
    }
    int sentence_count = read_sentences_from_file(filepath, validation_sentences, 100);
    
    printf("[SS] File has %d sentences. Requested sentence_num: %d (1-indexed)\n", sentence_count, sentence_num);
    
    // Check if the last sentence ends with a delimiter
    int last_sentence_complete = 0;
    if (sentence_count > 0) {
        char* last_sentence = validation_sentences[sentence_count - 1];
        int len = strlen(last_sentence);
        if (len > 0) {
            char last_char = last_sentence[len - 1];
            last_sentence_complete = (last_char == '.' || last_char == '!' || last_char == '?');
        }
    }
    
    printf("[SS] Last sentence complete (ends with delimiter): %s\n", last_sentence_complete ? "YES" : "NO");
    
    // Check if sentence_num is valid (1-based)
    // For empty files (sentence_count == 0), only sentence_num == 1 is valid (creates first sentence)
    // For non-empty files with complete last sentence, sentence_num can be from 1 to sentence_count+1 (appending allowed)
    // For non-empty files with incomplete last sentence, sentence_num can only be from 1 to sentence_count (no appending)
    int max_valid;
    if (sentence_count == 0) {
        max_valid = 1;
    } else if (last_sentence_complete) {
        max_valid = sentence_count + 1;  // Can append new sentence after a complete one
    } else {
        max_valid = sentence_count;  // Cannot append - must finish current sentence first
    }
    
    if (sentence_num < 1 || sentence_num > max_valid) {
        printf("[SS] Error: sentence out of range. Valid range: 1-%d\n", max_valid);
        char err_msg[128];
        snprintf(err_msg, sizeof(err_msg), "ERR_SENTENCE_OUT_OF_RANGE (Valid range: 1-%d)\n", max_valid);
        write(sock, err_msg, strlen(err_msg));
        free(validation_sentences);
        return;
    }
    
    free(validation_sentences);
    
    // 2. Read the current sentence content for locking
    char (*lock_sentences)[2048] = malloc(100 * sizeof(char[2048]));
    if (!lock_sentences) {
        write(sock, "ERR_MEMORY\n", 11);
        return;
    }
    int lock_sentence_count = read_sentences_from_file(filepath, lock_sentences, 100);
    
    char locked_sentence_content[2048] = "";
    if (sentence_num - 1 < lock_sentence_count) {
        strcpy(locked_sentence_content, lock_sentences[sentence_num - 1]);
    }
    free(lock_sentences);
    
    // 3. Now try to lock the sentence with its content
    if (!lock_sentence(filepath, sentence_num, locked_sentence_content)) {
        write(sock, "ERR_SENTENCE_LOCKED\n", 20);
        return;
    }

    // 4. Send ACK + original sentence content (trim leading whitespace for prefill)
    char prefill_sentence[2048] = "";
    const char *prefill_start = locked_sentence_content;
    while (*prefill_start && isspace((unsigned char)*prefill_start)) {
        prefill_start++;
    }
    strncpy(prefill_sentence, prefill_start, sizeof(prefill_sentence) - 1);
    prefill_sentence[sizeof(prefill_sentence) - 1] = '\0';
    write(sock, "ACK_WRITE_LOCKED\n", 17);
    write(sock, prefill_sentence, strlen(prefill_sentence));
    write(sock, "\n", 1);
    
    // 5. Read the full edited sentence
    char new_sentence_input[2048];
    int read_size = read(sock, new_sentence_input, sizeof(new_sentence_input) - 1);
    if (read_size <= 0) {
        write(sock, "ERR_WRITE_FAILED\n", 17);
        unlock_sentence(filepath, sentence_num);
        return;
    }
    new_sentence_input[read_size] = '\0';
    new_sentence_input[strcspn(new_sentence_input, "\r\n")] = '\0';
    
    // 6. Read the file and parse sentences again for processing
    char (*sentences)[2048] = malloc(100 * sizeof(char[2048]));
    if (!sentences) {
        write(sock, "ERR_MEMORY\n", 11);
        unlock_sentence(filepath, sentence_num);
        return;
    }
    sentence_count = read_sentences_from_file(filepath, sentences, 100);
    
    // 7. Find the actual sentence position by content (it may have moved!)
    int actual_sentence_num = sentence_num;
    if (strlen(locked_sentence_content) > 0) {
        actual_sentence_num = find_sentence_by_content(filepath, locked_sentence_content, sentence_num);
        if (actual_sentence_num < 0) {
            write(sock, "ERR_SENTENCE_MOVED_OR_DELETED\n", 30);
            unlock_sentence(filepath, sentence_num);
            free(sentences);
            return;
        }
    }
    
    int sentence_index = actual_sentence_num - 1;

    if (new_sentence_input[0] == '\0' && sentence_index < sentence_count) {
        strncpy(new_sentence_input, sentences[sentence_index], sizeof(new_sentence_input) - 1);
        new_sentence_input[sizeof(new_sentence_input) - 1] = '\0';
    }
    
    // Preserve leading space behavior for non-first sentences
    int has_leading_space = 0;
    if (sentence_index < sentence_count && sentence_count > 0) {
        has_leading_space = isspace(sentences[sentence_index][0]);
    } else if (sentence_index > 0) {
        has_leading_space = 1;
    }
    
    char new_sentence[2048] = "";
    if (has_leading_space && new_sentence_input[0] != ' ') {
        strncat(new_sentence, " ", sizeof(new_sentence) - 1);
    }
    strncat(new_sentence, new_sentence_input, sizeof(new_sentence) - strlen(new_sentence) - 1);
    
    // 9. Create backup for UNDO (only if file exists and has content)
    char bak_path[256];
    snprintf(bak_path, sizeof(bak_path), "%s.bak", filepath);
    
    // Copy original file to backup
    FILE* original = fopen(filepath, "r");
    if (original) {
        FILE* backup = fopen(bak_path, "w");
        if (backup) {
            char ch;
            while ((ch = fgetc(original)) != EOF) {
                fputc(ch, backup);
            }
            fclose(backup);
        }
        fclose(original);
    }
    
    // 10. Write the new file
    FILE* f = fopen(filepath, "w");
    if (!f) {
        printf("[SS] ERROR: Failed to open file for writing: %s\n", filepath);
        write(sock, "ERR_WRITE_FAILED\n", 17);
        unlock_sentence(filepath, sentence_num);
        free(sentences);
        return;
    }
    
    printf("[SS] Writing to file: %s\n", filepath);
    printf("[SS]   Sentences before modified: %d (0 to %d)\n", 
           (sentence_index < sentence_count ? sentence_index : 0), sentence_index > 0 ? sentence_index - 1 : 0);
    printf("[SS]   New sentence: '%s'\n", new_sentence);
    printf("[SS]   Sentences after modified: %d (%d to %d)\n", 
           (sentence_count > sentence_index + 1 ? sentence_count - sentence_index - 1 : 0),
           sentence_index + 1, sentence_count - 1);
    
    // Write all sentences before the modified one
    for (int i = 0; i < sentence_index && i < sentence_count; i++) {
        fprintf(f, "%s", sentences[i]);
        printf("[SS]   Wrote sentence %d: '%s'\n", i, sentences[i]);
    }
    
    // Write the new sentence (only if it has content)
    if (strlen(new_sentence) > 0) {
        fprintf(f, "%s", new_sentence);
        printf("[SS]   Wrote new sentence: '%s'\n", new_sentence);
    }
    
    // Write all sentences after the modified one
    for (int i = sentence_index + 1; i < sentence_count; i++) {
        fprintf(f, "%s", sentences[i]);
    }
    
    fflush(f);  // Ensure data is written to disk
    fclose(f);
    
    printf("[SS] File written successfully.\n");
    
    // 11. Unlock and send final ACK
    unlock_sentence(filepath, sentence_num);
    write(sock, "ACK_WRITE_SUCCESS\n", 18);
    
    // 12. Notify NM to trigger replication to other replicas
    // Extract just the filename from the full filepath
    const char* filename = strrchr(filepath, '/');
    if (filename != NULL) {
        filename++; // Skip the '/'
    } else {
        filename = filepath; // No slash found, use whole path
    }
    
    // Send async notification to NM (fire and forget)
    // Include SS_ID so NM knows which SS has the latest version
    extern char SS_ID[50];
    
    // Get file stats
    struct stat st;
    long file_size = 0;
    long total_words = 0;
    long char_count = 0;
    time_t last_access = 0;
    
    if (stat(filepath, &st) == 0) {
        file_size = st.st_size;
        char_count = st.st_size;
        last_access = st.st_atime;
        
        // Calculate word count
        FILE *fp = fopen(filepath, "r");
        if (fp) {
            int in_word = 0;
            int c;
            while ((c = fgetc(fp)) != EOF) {
                if (c == ' ' || c == '\n' || c == '\t') {
                    in_word = 0;
                } else if (!in_word) {
                    in_word = 1;
                    total_words++;
                }
            }
            fclose(fp);
        }
    }
    
    int nm_sock = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock >= 0) {
        char notify_msg[BUFFER_SIZE];
        snprintf(notify_msg, sizeof(notify_msg), "NM_FILE_MODIFIED %s %s %ld %ld %ld %ld\n", 
                 filename, SS_ID, file_size, total_words, char_count, last_access);
        write(nm_sock, notify_msg, strlen(notify_msg));
        close(nm_sock);
        printf("[SS] Notified NM about modification to %s from SS %s (size: %ld, words: %ld)\n", 
               filename, SS_ID, file_size, total_words);
    }
    
    // Free allocated memory
    free(sentences);
}


void handle_undo(int sock, const char* filepath) {
    char bak_path[BUFFER_SIZE];
    snprintf(bak_path, sizeof(bak_path), "%s.bak", filepath);

    // Try to rename .bak to the main file
    if (rename(bak_path, filepath) == 0) {
        printf("[SS] File %s reverted from backup.\n", filepath);
        write(sock, "ACK_UNDO_SUCCESS\n", 17);
    } else {
        perror("[SS] UNDO failed");
        // This can fail if there is no .bak file
        write(sock, "ERR_UNDO_FAILED\n", 16);
    }
}

// --- Helpers for checkpoints ---
static int mkdir_p(const char* path) {
    // Create directories recursively like `mkdir -p`
    char tmp[BUFFER_SIZE];
    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t len = strlen(tmp);
    if (len == 0) return 0;
    if (tmp[len-1] == '/') tmp[len-1] = '\0';
    for (char* p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) == -1 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) == -1 && errno != EEXIST) return -1;
    return 0;
}

static void build_checkpoint_paths(const char* filepath, char* dir_out, size_t dir_sz, char* file_out, size_t file_sz, const char* tag) {
    // Given full data file path: <SS_DATA_DIR>/<filename or folder/...>,
    // place checkpoints under <SS_DATA_DIR>/.checkpoints/<filename or folder/...>/<tag>.chk
    // Find SS_DATA_DIR prefix from filepath by searching last '/'
    // We assume filepath starts with SS_DATA_DIR
    const char* last_slash = strrchr(filepath, '/');
    // Derive base dir of data
    // Simply construct relative path from SS_DATA_DIR root by stripping SS_DATA_DIR prefix
    // SS_DATA_DIR is known only in storage_server.c; here we won't use it, we'll compute from filepath
    // Build dir_out by taking filepath and inserting '/.checkpoints' after the SS root component
    // Simpler: copy filepath into dir_out, then replace everything after the SS root with .checkpoints/<relative>
    // We'll approximate by prefixing ".checkpoints" after the first path component.

    // Copy filepath to work buffer
    char fp[BUFFER_SIZE];
    snprintf(fp, sizeof(fp), "%s", filepath);

    // Extract the first path component (SS root dir)
    char* first_slash = strchr(fp, '/');
    if (!first_slash) {
        // No slash? fall back to current directory
        snprintf(dir_out, dir_sz, ".checkpoints/%s", fp);
    } else {
        *first_slash = '\0';
        const char* root = fp; // e.g., ss_1_data
        const char* rest = first_slash + 1; // e.g., path/inside/file.txt
        snprintf(dir_out, dir_sz, "%s/.checkpoints/%s", root, rest);
    }

    // file path is dir_out + '/' + tag + ".chk"
    snprintf(file_out, file_sz, "%s/%s.chk", dir_out, tag);
}

void handle_checkpoint(int sock, const char* filepath, const char* tag) {
    if (!tag || strlen(tag) == 0) {
        write(sock, "ERR_BAD_CHECKPOINT_TAG\n", 23);
        return;
    }

    // Build paths
    char cp_dir[BUFFER_SIZE], cp_file[BUFFER_SIZE];
    build_checkpoint_paths(filepath, cp_dir, sizeof(cp_dir), cp_file, sizeof(cp_file), tag);

    if (mkdir_p(cp_dir) == -1) {
        write(sock, "ERR_CP_DIR_CREATE\n", 19);
        return;
    }

    // Read current file
    FILE* in = fopen(filepath, "r");
    if (!in) { write(sock, "ERR_SS_FILE_NOT_FOUND\n", 22); return; }
    FILE* out = fopen(cp_file, "w");
    if (!out) { fclose(in); write(sock, "ERR_CP_OPEN\n", 12); return; }
    int ch;
    while ((ch = fgetc(in)) != EOF) fputc(ch, out);
    fclose(in); fclose(out);
    write(sock, "ACK_CHECKPOINT\n", 15);
    
    // Notify NM to trigger replication (checkpoint doesn't change content, so skip for now)
    // Actually checkpoints are local to each SS, no need to replicate checkpoint metadata
}

void handle_viewcheckpoint(int sock, const char* filepath, const char* tag) {
    if (!tag || strlen(tag) == 0) { write(sock, "ERR_BAD_CHECKPOINT_TAG\n", 23); return; }
    char cp_dir[BUFFER_SIZE], cp_file[BUFFER_SIZE];
    build_checkpoint_paths(filepath, cp_dir, sizeof(cp_dir), cp_file, sizeof(cp_file), tag);
    FILE* f = fopen(cp_file, "r");
    if (!f) { write(sock, "ERR_CP_NOT_FOUND\n", 18); return; }
    char buffer[BUFFER_SIZE]; size_t n;
    while ((n = fread(buffer, 1, sizeof(buffer), f)) > 0) {
        if (write(sock, buffer, n) < 0) break;
    }
    fclose(f);
}

void handle_listcheckpoints(int sock, const char* filepath) {
    char cp_dir[BUFFER_SIZE], cp_file_dummy[BUFFER_SIZE];
    build_checkpoint_paths(filepath, cp_dir, sizeof(cp_dir), cp_file_dummy, sizeof(cp_file_dummy), "__dummy__");
    
    // cp_dir now contains the full directory path for checkpoints
    // cp_file_dummy contains: cp_dir/__dummy__.chk
    // We need just the directory part from cp_file_dummy
    // So we strip "/__dummy__.chk" by finding the directory portion of cp_file_dummy
    
    // Actually, let's use cp_file_dummy and strip the filename
    char* last_slash = strrchr(cp_file_dummy, '/');
    if (last_slash) {
        *last_slash = '\0';  // Now cp_file_dummy contains just the directory
    }

    // Open directory and list *.chk files
    DIR* d = opendir(cp_file_dummy);
    if (!d) { 
        printf("[SS] DEBUG: Cannot open checkpoint dir: %s (errno: %d)\n", cp_file_dummy, errno);
        write(sock, "(no checkpoints)\n", 18); 
        return; 
    }
    
    struct dirent* de;
    char line[BUFFER_SIZE];
    int found_count = 0;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        // expect files named <tag>.chk
        const char* name = de->d_name;
        size_t len = strlen(name);
        if (len > 4 && strcmp(name + len - 4, ".chk") == 0) {
            snprintf(line, sizeof(line), "%.*s\n", (int)(len - 4), name);
            write(sock, line, strlen(line));
            found_count++;
        }
    }
    closedir(d);
    
    if (found_count == 0) {
        write(sock, "(no checkpoints)\n", 18);
    }
}

void handle_revert_to_checkpoint(int sock, const char* filepath, const char* tag) {
    if (!tag || strlen(tag) == 0) { write(sock, "ERR_BAD_CHECKPOINT_TAG\n", 23); return; }
    char cp_dir[BUFFER_SIZE], cp_file[BUFFER_SIZE];
    build_checkpoint_paths(filepath, cp_dir, sizeof(cp_dir), cp_file, sizeof(cp_file), tag);

    FILE* in = fopen(cp_file, "r");
    if (!in) { write(sock, "ERR_CP_NOT_FOUND\n", 18); return; }

    // Backup current file for UNDO compatibility
    char bak_path[BUFFER_SIZE];
    snprintf(bak_path, sizeof(bak_path), "%s.bak", filepath);
    FILE* cur = fopen(filepath, "r");
    if (cur) {
        FILE* bak = fopen(bak_path, "w");
        if (bak) {
            int ch; while ((ch = fgetc(cur)) != EOF) fputc(ch, bak);
            fclose(bak);
        }
        fclose(cur);
    }

    FILE* out = fopen(filepath, "w");
    if (!out) { fclose(in); write(sock, "ERR_REVERT_OPEN\n", 16); return; }
    int ch; while ((ch = fgetc(in)) != EOF) fputc(ch, out);
    fclose(in); fclose(out);
    write(sock, "ACK_REVERT\n", 11);
    
    // Notify NM to trigger replication to other replicas
    const char* filename = strrchr(filepath, '/');
    if (filename != NULL) {
        filename++; // Skip the '/'
    } else {
        filename = filepath;
    }
    
    extern char SS_ID[50];
    
    // Get file stats
    struct stat st;
    long file_size = 0;
    long total_words = 0;
    long char_count = 0;
    time_t last_access = 0;
    
    if (stat(filepath, &st) == 0) {
        file_size = st.st_size;
        char_count = st.st_size;
        last_access = st.st_atime;
        
        // Calculate word count
        FILE *fp = fopen(filepath, "r");
        if (fp) {
            int in_word = 0;
            int c;
            while ((c = fgetc(fp)) != EOF) {
                if (c == ' ' || c == '\n' || c == '\t') {
                    in_word = 0;
                } else if (!in_word) {
                    in_word = 1;
                    total_words++;
                }
            }
            fclose(fp);
        }
    }
    
    int nm_sock = connect_to_server(NM_IP, NM_PORT);
    if (nm_sock >= 0) {
        char notify_msg[BUFFER_SIZE];
        snprintf(notify_msg, sizeof(notify_msg), "NM_FILE_MODIFIED %s %s %ld %ld %ld %ld\n", 
                 filename, SS_ID, file_size, total_words, char_count, last_access);
        write(nm_sock, notify_msg, strlen(notify_msg));
        close(nm_sock);
        printf("[SS] Notified NM about revert of %s from SS %s (size: %ld, words: %ld)\n", 
               filename, SS_ID, file_size, total_words);
    }
}