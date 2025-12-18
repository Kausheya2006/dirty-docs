#include "../common/utils.h"
#include "../common/config.h"
#include <readline/readline.h>
#include <readline/history.h>

// --- ANSI Color Codes ---
#define RESET       "\033[0m"
#define BOLD        "\033[1m"
#define RED         "\033[31m"
#define GREEN       "\033[32m"
#define YELLOW      "\033[33m"
#define BLUE        "\033[34m"
#define MAGENTA     "\033[35m"
#define CYAN        "\033[36m"
#define WHITE       "\033[37m"

// --- Box Drawing Characters (Single Line) ---
#define TOP_LEFT     "┌"
#define TOP_RIGHT    "┐"
#define BOTTOM_LEFT  "└"
#define BOTTOM_RIGHT "┘"
#define HORIZONTAL   "─"
#define VERTICAL     "│"
#define T_RIGHT      "├"
#define T_LEFT       "┤"
#define T_DOWN       "┬"
#define T_UP         "┴"
#define CROSS        "┼"

// --- Helper Functions for Pretty Output ---
void print_separator(int width) {
    for (int i = 0; i < width; i++) printf("%s", HORIZONTAL);
}

void print_box_top(int width) {
    printf("%s", TOP_LEFT);
    print_separator(width);
    printf("%s\n", TOP_RIGHT);
}

void print_box_bottom(int width) {
    printf("%s", BOTTOM_LEFT);
    print_separator(width);
    printf("%s\n", BOTTOM_RIGHT);
}

void print_box_middle(int width) {
    printf("%s", T_RIGHT);
    print_separator(width);
    printf("%s\n", T_LEFT);
}

void print_box_line(const char* content, int width, const char* color) {
    int content_len = strlen(content);
    int padding = width - content_len - 2;
    printf("%s%s%s %s%s", color, BOLD, VERTICAL, RESET, content);
    for (int i = 0; i < padding; i++) printf(" ");
    printf("%s%s %s%s\n", color, BOLD, VERTICAL, RESET);
}

void print_centered_line(const char* content, int width, const char* color) {
    int content_len = strlen(content);
    int total_padding = width - content_len - 2;
    int left_padding = total_padding / 2;
    int right_padding = total_padding - left_padding;
    
    printf("%s%s%s ", color, BOLD, VERTICAL);
    for (int i = 0; i < left_padding; i++) printf(" ");
    printf("%s%s%s", color, content, RESET);
    for (int i = 0; i < right_padding; i++) printf(" ");
    printf("%s%s%s%s\n", color, BOLD, VERTICAL, RESET);
}

void print_welcome_banner() {
    printf("\n%s%s", CYAN, BOLD);
    
    // Figlet-style ASCII art for "DirtyDocs++"
    printf(" ____  _      _         ____                  \n");
    printf("|  _ \\(_)_ __| |_ _   _|  _ \\  ___   ___ ___   _     _\n");
    printf("| | | | | '__| __| | | | | | |/ _ \\ / __/ __|_| |_ _| |_ \n");
    printf("| |_| | | |  | |_| |_| | |_| | (_) | (__\\__ \\_   _|_   _| \n");
    printf("|____/|_|_|   \\__|\\__, |____/ \\___/ \\___|___/ |_|   |_|\n");
    printf("                  |___/                                   \n");
    
    printf("%s", RESET);
    
    int width = 62;
    printf("%s%s", CYAN, BOLD);
    print_box_top(width);
    print_centered_line("A Distributed Collaborative Document Editor", width+1, WHITE);
    print_box_bottom(width);
    printf("%s\n", RESET);
}

void print_man_page(const char* cmd) {
    int width = 70;
    char title[100];
    snprintf(title, sizeof(title), "MANUAL: %s", cmd);
    
    printf("\n%s%s", CYAN, BOLD);
    print_box_top(width);
    print_centered_line(title, width+1, CYAN);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // Match command and print detailed manual
    if (strcasecmp(cmd, "CREATE") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  CREATE <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Creates a new empty file with the specified name.", width, RESET);
        print_box_line("  The file is owned by the user who creates it and is", width, RESET);
        print_box_line("  initially empty. The creator has full read and write", width, RESET);
        print_box_line("  permissions.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  The file is stored on a storage server and tracked by", width, RESET);
        print_box_line("  the name server. After creation, use WRITE to add", width, RESET);
        print_box_line("  content or ADDACCESS to share with other users.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  CREATE myfile.txt", width, RESET);
        print_box_line("  CREATE document.doc", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  DELETE, WRITE, ADDACCESS, INFO", width, RESET);
    }
    else if (strcasecmp(cmd, "DELETE") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  DELETE <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Permanently deletes the specified file. Only the file", width, RESET);
        print_box_line("  owner can delete a file. This action cannot be undone.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  NOTE: For safer deletion, use TRASH instead. TRASH", width, RESET);
        print_box_line("  moves files to the recycle bin where they can be", width, RESET);
        print_box_line("  restored. DELETE permanently removes files without", width, RESET);
        print_box_line("  recovery option.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  All checkpoints associated with the file are also", width, RESET);
        print_box_line("  deleted. Users who had access will no longer be able", width, RESET);
        print_box_line("  to access the file.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  DELETE myfile.txt", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("WARNING", width, CYAN);
        print_box_line("  This operation is irreversible!", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  TRASH, EMPTYTRASH", width, RESET);
    }
    else if (strcasecmp(cmd, "TRASH") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  TRASH <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Moves the specified file to the recycle bin (trash).", width, RESET);
        print_box_line("  Only the file owner can trash their files. Files in", width, RESET);
        print_box_line("  trash are hidden from normal view but can be restored.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Trashed files:", width, RESET);
        width+=2;
        print_box_line("  • Are not visible in VIEW or VIEWFOLDER", width, RESET);
        print_box_line("  • Cannot be accessed by READ, WRITE, or other ops", width, RESET);
        print_box_line("  • Can be viewed using VIEWTRASH", width, RESET);
        print_box_line("  • Can be restored using RESTORE", width, RESET);
        print_box_line("  • Are permanently deleted using EMPTYTRASH", width, RESET);
        width-=2;
        print_box_line("  ", width, RESET);
        print_box_line("  This is safer than DELETE as it allows recovery.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  TRASH myfile.txt", width, RESET);
        print_box_line("  TRASH old_document.txt", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  RESTORE, VIEWTRASH, EMPTYTRASH, DELETE", width, RESET);
    }
    else if (strcasecmp(cmd, "RESTORE") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  RESTORE <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Restores a file from the recycle bin back to active", width, RESET);
        print_box_line("  state. Only the file owner can restore their files.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  After restoration, the file:", width, RESET);
        width+=2;
        print_box_line("  • Becomes visible in VIEW and VIEWFOLDER again", width, RESET);
        print_box_line("  • Can be accessed normally by all permitted users", width, RESET);
        print_box_line("  • Retains all its original permissions and metadata", width, RESET);
        width-=2;
        print_box_line("  ", width, RESET);
        print_box_line("  Use VIEWTRASH to see which files are in your trash.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  RESTORE myfile.txt", width, RESET);
        print_box_line("  RESTORE important_doc.txt", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  TRASH, VIEWTRASH, EMPTYTRASH", width, RESET);
    }
    else if (strcasecmp(cmd, "VIEWTRASH") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  VIEWTRASH", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Lists all files currently in your recycle bin.", width, RESET);
        print_box_line("  Shows only files you own that have been trashed.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Files in trash:", width, RESET);
        width+=2;
        print_box_line("  • Are hidden from normal VIEW command", width, RESET);
        print_box_line("  • Can be restored using RESTORE <filename>", width, RESET);
        print_box_line("  • Can be permanently deleted using EMPTYTRASH", width, RESET);
        width-=2;
        print_box_line("  ", width, RESET);
        print_box_line("  Each user has their own separate trash bin.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  VIEWTRASH", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  TRASH, RESTORE, EMPTYTRASH", width, RESET);
    }
    else if (strcasecmp(cmd, "EMPTYTRASH") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  EMPTYTRASH", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Permanently deletes ALL files in your recycle bin.", width, RESET);
        print_box_line("  This action cannot be undone.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Only affects files you own. Other users' trashed", width, RESET);
        print_box_line("  files are not affected.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  The command will:", width, RESET);
        width+=2;
        print_box_line("  • Delete all trashed files from storage servers", width, RESET);
        print_box_line("  • Remove all trashed files from the file system", width, RESET);
        print_box_line("  • Display count of files permanently deleted", width, RESET);
        width-=2;
        print_box_line("  ", width, RESET);
        print_box_line("  Use VIEWTRASH first to see what will be deleted.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  EMPTYTRASH", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("WARNING", width, CYAN);
        print_box_line("  This operation is irreversible! All trashed files", width, RESET);
        print_box_line("  will be permanently deleted.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  VIEWTRASH, TRASH, RESTORE", width, RESET);
    }
    else if (strcasecmp(cmd, "READ") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  READ <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays the entire contents of the specified file in", width, RESET);
        print_box_line("  a formatted box. Requires READ permission (granted by", width, RESET);
        print_box_line("  file owner or implicit if you own the file).", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  The file content is retrieved from the storage server", width, RESET);
        print_box_line("  and displayed in a single request. For large files,", width, RESET);
        print_box_line("  consider using STREAM for word-by-word display.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  READ myfile.txt", width, RESET);
        print_box_line("  READ shared/document.txt", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  STREAM, WRITE, INFO", width, RESET);
    }
    else if (strcasecmp(cmd, "WRITE") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  WRITE <filename> <sentence_number>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Opens an interactive mode to edit a specific sentence", width, RESET);
        print_box_line("  in the file. Sentences are 1-indexed. Words are also", width, RESET);
        print_box_line("  1-indexed. Enter word updates as '<position> <content>'", width, RESET);
        print_box_line("  and type 'ETIRW' (WRITE backwards) when done.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  The sentence is locked during editing to prevent", width, RESET);
        print_box_line("  concurrent modifications. Requires WRITE permission.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Position 1 = first word, Position N+1 = append after", width, RESET);
        print_box_line("  last word. Content can include multiple words.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  WRITE myfile.txt 1", width, RESET);
        print_box_line("  1 Hello World.", width, RESET);
        print_box_line("  ETIRW", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  WRITE myfile.txt 2", width, RESET);
        print_box_line("  1 This", width, RESET);
        print_box_line("  2 is a test.", width, RESET);
        print_box_line("  ETIRW", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("NOTES", width, CYAN);
        print_box_line("  - Both sentences and words use 1-based indexing", width, RESET);
        print_box_line("  - Sentences are delimited by . ! or ?", width, RESET);
        print_box_line("  - Use UNDO command to revert last WRITE", width, RESET);
    }
    else if (strcasecmp(cmd, "STREAM") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  STREAM <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays file content word-by-word with delays.", width, RESET);
        print_box_line("  Requires READ permission.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  STREAM myfile.txt", width, RESET);
    }
    else if (strcasecmp(cmd, "UNDO") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  UNDO <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Reverts the last WRITE operation on the file.", width, RESET);
        print_box_line("  Requires WRITE permission.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  UNDO myfile.txt", width, RESET);
    }
    else if (strcasecmp(cmd, "CHECKPOINT") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  CHECKPOINT <filename> <tag>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Creates a named checkpoint (snapshot) of the file at", width, RESET);
        print_box_line("  its current state. The tag identifies this checkpoint", width, RESET);
        print_box_line("  for later viewing or restoration.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Checkpoints are stored separately from the main file", width, RESET);
        print_box_line("  and persist even after modifications. Use REVERT to", width, RESET);
        print_box_line("  restore a file to a checkpoint state.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Multiple checkpoints can exist for a single file,", width, RESET);
        print_box_line("  each with a unique tag.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  CHECKPOINT myfile.txt v1", width, RESET);
        print_box_line("  CHECKPOINT myfile.txt backup-before-major-edit", width, RESET);
        print_box_line("  CHECKPOINT doc.txt 2024-11-06", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  VIEWCHECKPOINT, LISTCHECKPOINTS, REVERT", width, RESET);
    }
    else if (strcasecmp(cmd, "VIEWCHECKPOINT") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  VIEWCHECKPOINT <filename> <tag>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays the content of a specific checkpoint", width, RESET);
        print_box_line("  without modifying the current file.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  VIEWCHECKPOINT myfile.txt v1", width, RESET);
    }
    else if (strcasecmp(cmd, "LISTCHECKPOINTS") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  LISTCHECKPOINTS <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Lists all available checkpoint tags for a file.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  LISTCHECKPOINTS myfile.txt", width, RESET);
    }
    else if (strcasecmp(cmd, "REVERT") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  REVERT <filename> <tag>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Restores file to a previous checkpoint state.", width, RESET);
        print_box_line("  This replaces the current file content.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  REVERT myfile.txt v1", width, RESET);
    }
    else if (strcasecmp(cmd, "VIEW") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  VIEW", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Lists all files you have access to, including", width, RESET);
        print_box_line("  files you own and files shared with you.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  VIEW", width, RESET);
    }
    else if (strcasecmp(cmd, "INFO") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  INFO <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays comprehensive metadata about a file in a", width, RESET);
        print_box_line("  beautifully formatted box. Information includes:", width, RESET);
        print_box_line("  ", width, RESET);
        width+=2;
        print_box_line("  • File name", width, RESET);
        print_box_line("  • Owner username", width, RESET);
        print_box_line("  • File size in bytes", width, RESET);
        print_box_line("  • Creation timestamp", width, RESET);
        print_box_line("  • List of users with write access", width, RESET);
        print_box_line("  • List of users with read access", width, RESET);
        width-=2;
        print_box_line("  ", width, RESET);
        print_box_line("  Requires at least READ permission to view info.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  INFO myfile.txt", width, RESET);
        print_box_line("  INFO shared/document.txt", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  VIEW, READ, ADDACCESS, REMACCESS", width, RESET);
    }
    else if (strcasecmp(cmd, "LIST") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  LIST", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Shows all users currently connected to the", width, RESET);
        print_box_line("  name server.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  LIST", width, RESET);
    }
    else if (strcasecmp(cmd, "ADDACCESS") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  ADDACCESS -R <filename> <username>", width, RESET);
        print_box_line("  ADDACCESS -W <filename> <username>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Grants access permissions to another user. Only the", width, RESET);
        print_box_line("  file owner can grant access. Two permission levels:", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  -R : Read-only access (user can view file content)", width, RESET);
        print_box_line("  -W : Write access (user can modify file, implies", width, RESET);
        print_box_line("       read access)", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Write access allows: WRITE, UNDO, CHECKPOINT, REVERT", width, RESET);
        print_box_line("  Read access allows: READ, STREAM, INFO, VIEWCHECKPOINT", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Use REMACCESS to revoke permissions.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  ADDACCESS -R myfile.txt alice", width, RESET);
        print_box_line("  ADDACCESS -W shared.txt bob", width, RESET);
        print_box_line("  ADDACCESS -R document.txt charlie", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  REMACCESS, REQACCESS, INFO", width, RESET);
    }
    else if (strcasecmp(cmd, "REMACCESS") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  REMACCESS <filename> <username>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Revokes all access permissions from a user.", width, RESET);
        print_box_line("  Only the file owner can revoke access.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  REMACCESS myfile.txt alice", width, RESET);
    }
    else if (strcasecmp(cmd, "CREATEFOLDER") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  CREATEFOLDER <foldername>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Creates a new folder for organizing files.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  CREATEFOLDER documents", width, RESET);
    }
    else if (strcasecmp(cmd, "MOVE") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  MOVE <filename> <destination>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Moves a file to the specified location. Only the file", width, RESET);
        print_box_line("  owner can move their files. The destination folder", width, RESET);
        print_box_line("  must exist before moving the file.", width, RESET);
        print_box_line("  ", width, RESET);
        print_box_line("  Destination formats:", width, RESET);
        width+=2;
        print_box_line("  • folder_name  - Move to a folder", width, RESET);
        print_box_line("  • .            - Move to root directory", width, RESET);
        width-=2;
        print_box_line("", width, RESET);
        print_box_line("EXAMPLES", width, CYAN);
        print_box_line("  MOVE myfile.txt documents", width, RESET);
        print_box_line("  MOVE docs/file.txt .    # Move to root", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("SEE ALSO", width, CYAN);
        print_box_line("  CREATEFOLDER, VIEWFOLDER", width, RESET);
    }
    else if (strcasecmp(cmd, "VIEWFOLDER") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  VIEWFOLDER <foldername>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Lists all files contained in a folder.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  VIEWFOLDER documents", width, RESET);
    }
    else if (strcasecmp(cmd, "EXEC") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  EXEC <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Executes a shell script and stores output in", width, RESET);
        print_box_line("  the specified output file.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  EXEC script.sh output.txt", width, RESET);
    }
    else if (strcasecmp(cmd, "REQACCESS") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  REQACCESS -R <filename>", width, RESET);
        print_box_line("  REQACCESS -W <filename>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Sends a request to the file owner asking for", width, RESET);
        print_box_line("  read (-R) or write (-W) access permission.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  REQACCESS -R shared.txt", width, RESET);
        print_box_line("  REQACCESS -W documents.txt", width, RESET);
    }
    else if (strcasecmp(cmd, "LISTREQ") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  LISTREQ", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays all access requests where you are", width, RESET);
        print_box_line("  either the requester or the file owner.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  LISTREQ", width, RESET);
    }
    else if (strcasecmp(cmd, "APPROVE") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  APPROVE <request_id>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Approves an access request. Only the file owner", width, RESET);
        print_box_line("  can approve requests. Use LISTREQ to see request IDs.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  APPROVE 3", width, RESET);
    }
    else if (strcasecmp(cmd, "DENY") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  DENY <request_id>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Denies an access request. Only the file owner", width, RESET);
        print_box_line("  can deny requests. Use LISTREQ to see request IDs.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  DENY 3", width, RESET);
    }
    else if (strcasecmp(cmd, "help") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  help", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays a list of all available commands with", width, RESET);
        print_box_line("  brief descriptions.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  help", width, RESET);
    }
    else if (strcasecmp(cmd, "man") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  man <command>", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Displays detailed manual/documentation for the", width, RESET);
        print_box_line("  specified command including usage and examples.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  man CREATE", width, RESET);
        print_box_line("  man WRITE", width, RESET);
    }
    else if (strcasecmp(cmd, "exit") == 0) {
        print_box_line("SYNOPSIS", width, CYAN);
        print_box_line("  exit", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("DESCRIPTION", width, CYAN);
        print_box_line("  Disconnects from the server and exits the client.", width, RESET);
        print_box_line("", width, RESET);
        print_box_line("EXAMPLE", width, CYAN);
        print_box_line("  exit", width, RESET);
    }
    else {
        print_box_line("ERROR: Unknown command", width, RED);
        print_box_line("", width, RESET);
        print_box_line("Type 'help' to see all available commands.", width, RESET);
    }
    
    printf("%s%s", CYAN, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
}

void print_help() {
    int width = 62;
    
    printf("\n%s%s", CYAN, BOLD);
    print_box_top(width+1);
    print_centered_line("AVAILABLE COMMANDS", width+2, CYAN);
    print_box_middle(width+1);
    // printf("%s\n", RESET);
    
    // Header
    printf("%s%s%s %-26s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, "COMMAND", VERTICAL, "DESCRIPTION", VERTICAL, RESET);
    printf("%s%s", CYAN, BOLD);
    print_box_middle(width+1);
    // printf("%s\n", RESET);
    
    // File Operations
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "CREATE <filename>", RESET, VERTICAL, "Create a new file", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "TRASH <filename>", RESET, VERTICAL, "Move file to trash", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "READ <filename>", RESET, VERTICAL, "Read file contents", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "WRITE <file> <sentence#>", RESET, VERTICAL, "Edit a sentence in file", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "STREAM <filename>", RESET, VERTICAL, "Stream file word-by-word", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "UNDO <filename>", RESET, VERTICAL, "Undo last write", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "CHECKPOINT <file> <tag>", RESET, VERTICAL, "Save a named checkpoint", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "VIEWCHECKPOINT <f> <tag>", RESET, VERTICAL, "View a checkpoint content", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "LISTCHECKPOINTS <file>", RESET, VERTICAL, "List checkpoint tags", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "REVERT <file> <tag>", RESET, VERTICAL, "Revert to a checkpoint", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "VIEWTRASH", RESET, VERTICAL, "List files in trash", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "RESTORE <filename>", RESET, VERTICAL, "Restore file from trash", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "EMPTYTRASH", RESET, VERTICAL, "Permanently delete trash", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "VIEW", RESET, VERTICAL, "List all files", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "INFO <filename>", RESET, VERTICAL, "Show file metadata", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, GREEN, "LIST", RESET, VERTICAL, "List connected users", VERTICAL, RESET);
    
    printf("%s%s", CYAN, BOLD);
    print_box_middle(width+1);
    // printf("%s\n", RESET);
    
    // Access Control
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, YELLOW, "ADDACCESS -R <file> <u>", RESET, VERTICAL, "Grant READ access", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, YELLOW, "ADDACCESS -W <file> <u>", RESET, VERTICAL, "Grant WRITE access", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, YELLOW, "REMACCESS <file> <user>", RESET, VERTICAL, "Revoke access", VERTICAL, RESET);
    
    printf("%s%s", CYAN, BOLD);
    print_box_middle(width+1);
    // printf("%s\n", RESET);
    
    // Folder Operations
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, MAGENTA, "CREATEFOLDER <name>", RESET, VERTICAL, "Create a new folder", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, MAGENTA, "MOVE <file> <folder|.>", RESET, VERTICAL, "Move file (. = root)", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, MAGENTA, "VIEWFOLDER <name>", RESET, VERTICAL, "List folder contents", VERTICAL, RESET);
    
    printf("%s%s", CYAN, BOLD);
    print_box_middle(width+1);
    // printf("%s\n", RESET);
    
    // Other
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "EXEC <filename>", RESET, VERTICAL, "Execute shell script", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "help", RESET, VERTICAL, "Show this help", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "man <COMMAND>", RESET, VERTICAL, "Manual for a command", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "REQACCESS -R|-W <file>", RESET, VERTICAL, "Request access to a file", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "LISTREQ", RESET, VERTICAL, "View your access requests", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "APPROVE <id>", RESET, VERTICAL, "Approve a request (owner)", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, BLUE, "DENY <id>", RESET, VERTICAL, "Deny a request (owner)", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, RED, "DELETE <filename>", RESET, VERTICAL, "Permanently delete a file", VERTICAL, RESET);
    printf("%s%s%s %s%-26s%s %s %-32s %s%s\n", CYAN, BOLD, VERTICAL, RED, "exit", RESET, VERTICAL, "Disconnect and quit", VERTICAL, RESET);
    
    printf("%s%s", CYAN, BOLD);
    print_box_bottom(width+1);
    // printf("%s\n", RESET);
    
    printf("%s%sTIP:%s Type 'help' to see this list again.\n", YELLOW, BOLD, RESET);
    printf("%s%sNOTE:%s WRITE: <position> <content>, then ETIRW to save.\n\n", YELLOW, BOLD, RESET);
}

void print_file_list(const char* files) {
    int width = 100;
    
    printf("\n%s%s", GREEN, BOLD);
    print_box_top(width);
    print_centered_line("AVAILABLE FILES & FOLDERS", width+1, GREEN);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // Parse and display files line by line
    char temp[BUFFER_SIZE * 4];
    strncpy(temp, files, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    char *line = strtok(temp, "\n");
    int count = 0;
    while (line != NULL) {
        if (strlen(line) > 0) {
            // Wrap long lines instead of truncating
            int line_len = strlen(line);
            int pos = 0;
            while (pos < line_len) {
                char display_line[99];
                int remaining = line_len - pos;
                int to_copy = (remaining > 98) ? 98 : remaining;
                strncpy(display_line, line + pos, to_copy);
                display_line[to_copy] = '\0';
                printf("%s%s%s %s%-98s %s%s\n", GREEN, BOLD, VERTICAL, RESET, display_line, GREEN, VERTICAL);
                pos += to_copy;
            }
            count++;
        }
        line = strtok(NULL, "\n");
    }
    
    if (count == 0) {
        print_box_line("No files available", width, GREEN);
    }
    
    printf("%s%s", GREEN, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
    fflush(stdout);
}

void print_user_list(const char* users, const char* current_username) {
    int width = 62;
    
    printf("\n%s%s", BLUE, BOLD);
    print_box_top(width);
    print_centered_line("CONNECTED USERS", width+1, BLUE);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // Parse and display users line by line
    char temp[BUFFER_SIZE];
    strncpy(temp, users, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    char *line = strtok(temp, "\n");
    int count = 0;
    while (line != NULL) {
        if (strlen(line) > 0 && strcmp(line, "Users:") != 0) {
            // Trim leading/trailing whitespace
            while (*line == ' ' || *line == '\t') line++;
            char *end = line + strlen(line) - 1;
            while (end > line && (*end == ' ' || *end == '\t' || *end == '\n')) {
                *end = '\0';
                end--;
            }
            
            // Check if this is the current user
            int is_current = (strcmp(line, current_username) == 0);
            
            // Prepare display with indicator
            char display_text[256];
            const char *color = RESET;
            const char *indicator = "";
            
            if (is_current) {
                snprintf(display_text, sizeof(display_text), "→ %s (you)", line);
                color = GREEN;
            } else {
                snprintf(display_text, sizeof(display_text), "• %s", line);
                color = CYAN;
            }
            
            // Wrap long lines
            int line_len = strlen(display_text);
            int pos = 0;
            while (pos < line_len) {
                char display_line[61];
                int remaining = line_len - pos;
                int to_copy = (remaining > 60) ? 60 : remaining;
                strncpy(display_line, display_text + pos, to_copy);
                display_line[to_copy] = '\0';
                printf("%s%s%s %s%s%s", BLUE, BOLD, VERTICAL, color, display_line, RESET);
                int padding = 60 - to_copy + 2;
                for (int i = 0; i < padding; i++) printf(" ");
                printf(" %s%s%s\n", BLUE, VERTICAL, RESET);
                pos += to_copy;
            }
            count++;
        }
        line = strtok(NULL, "\n");
    }
    
    if (count == 0) {
        print_box_line("No other users connected", width, BLUE);
    }
    
    printf("%s%s", BLUE, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
    fflush(stdout);
}

void print_file_info(const char* info_data) {
    int width = 62;
    char filename[256] = "", owner[256] = "", size[256] = "", created[256] = "";
    char write_access[1024] = "", read_access[1024] = "";
    
    // Parse the info data
    char temp[BUFFER_SIZE * 2];
    strncpy(temp, info_data, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    char *line = strtok(temp, "\n");
    while (line != NULL) {
        if (strncmp(line, "FILE:", 5) == 0) {
            strcpy(filename, line + 5);
        } else if (strncmp(line, "OWNER:", 6) == 0) {
            strcpy(owner, line + 6);
        } else if (strncmp(line, "SIZE:", 5) == 0) {
            strcpy(size, line + 5);
        } else if (strncmp(line, "CREATED:", 8) == 0) {
            strcpy(created, line + 8);
        } else if (strncmp(line, "WRITE_ACCESS:", 13) == 0) {
            strcpy(write_access, line + 13);
        } else if (strncmp(line, "READ_ACCESS:", 12) == 0) {
            strcpy(read_access, line + 12);
        }
        line = strtok(NULL, "\n");
    }
    
    // Add dash if fields are empty
    if (strlen(filename) == 0) strcpy(filename, "-");
    if (strlen(owner) == 0) strcpy(owner, "-");
    if (strlen(size) == 0) strcpy(size, "-");
    if (strlen(created) == 0) strcpy(created, "-");
    if (strlen(write_access) == 0 || strcmp(write_access, "(none)") == 0) strcpy(write_access, "-");
    if (strlen(read_access) == 0 || strcmp(read_access, "(none)") == 0) strcpy(read_access, "-");
    
    // Print with multiple colors in box format
    printf("\n%s%s", CYAN, BOLD);
    print_box_top(width);
    print_centered_line("FILE INFORMATION", width+1, CYAN);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // File details - white bold labels with bright colored values
    char display_line[300];
    int len, padding;
    
    // File
    len = strlen(filename) + 12; // "File:      " = 11 chars + space
    printf("%s%s%s %s%sFile:%s      %s%s%s", CYAN, BOLD, VERTICAL, RESET, BOLD, RESET, CYAN, filename, RESET);
    padding = 60 - len + 1;
    for (int i = 0; i < padding; i++) printf(" ");
    printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    
    // Owner
    len = strlen(owner) + 12;
    printf("%s%s%s %s%sOwner:%s     %s%s%s", MAGENTA, BOLD, VERTICAL, RESET, BOLD, RESET, GREEN, owner, RESET);
    padding = 60 - len + 1;
    for (int i = 0; i < padding; i++) printf(" ");
    printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    
    // Size
    len = strlen(size) + 18; // "Size:      " + " bytes"
    printf("%s%s%s %s%sSize:%s      %s%s bytes%s", CYAN, BOLD, VERTICAL, RESET, BOLD, RESET, YELLOW, size, RESET);
    padding = 60 - len + 1;
    for (int i = 0; i < padding; i++) printf(" ");
    printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    
    // Created
    len = strlen(created) + 12;
    printf("%s%s%s %s%sCreated:%s   %s%s%s", CYAN, BOLD, VERTICAL, RESET, BOLD, RESET, CYAN, created, RESET);
    padding = 60 - len + 1;
    for (int i = 0; i < padding; i++) printf(" ");
    printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    
    // Separator
    printf("%s%s", CYAN, BOLD);
    print_box_middle(width);
    print_centered_line("ACCESS PERMISSIONS", width+1 , CYAN);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // Write access
    printf("%s%s%s %s%sWrite Access:%s", CYAN, BOLD, VERTICAL, RESET, BOLD, RESET);
    padding = 60 - 13; // "Write Access:" = 13 chars
    for (int i = 0; i < padding; i++) printf(" ");
    printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    
    if (strcmp(write_access, "-") == 0) {
        printf("%s%s%s   %s-%s", CYAN, BOLD, VERTICAL, RED, RESET);
        padding = 60 - 4 + 1; // "  -" = 3 chars + space
        for (int i = 0; i < padding; i++) printf(" ");
        printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    } else {
        // Parse comma-separated list
        char write_temp[1024];
        strcpy(write_temp, write_access);
        char *user = strtok(write_temp, ",");
        while (user != NULL) {
            // Trim leading spaces
            while (*user == ' ') user++;
            len = strlen(user) + 4; // "  • " = 4 chars
            printf("%s%s%s   %s•%s %s%s%s", CYAN, BOLD, VERTICAL, RESET, RESET, CYAN, user, RESET);
            padding = 60 - len;
            for (int i = 0; i < padding; i++) printf(" ");
            printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
            user = strtok(NULL, ",");
        }
    }
    
    // Empty line
    printf("%s%s%s %s%-60s %s%s\n", CYAN, BOLD, VERTICAL, RESET, "", CYAN, VERTICAL);

    // Read access
    printf("%s%s%s %s%sRead Access:%s", CYAN, BOLD, VERTICAL, RESET, BOLD, RESET);
    padding = 60 - 12; // "Read Access:" = 12 chars
    for (int i = 0; i < padding; i++) printf(" ");
    printf(" %s%s%s\n", CYAN, VERTICAL, RESET);

    if (strcmp(read_access, "-") == 0) {
        printf("%s%s%s   %s-%s", CYAN, BOLD, VERTICAL, RED, RESET);
        padding = 60 - 4 + 1;
        for (int i = 0; i < padding; i++) printf(" ");
        printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
    } else {
        // Parse comma-separated list
        char read_temp[1024];
        strcpy(read_temp, read_access);
        char *user = strtok(read_temp, ",");
        while (user != NULL) {
            // Trim leading spaces
            while (*user == ' ') user++;
            len = strlen(user) + 4;
            printf("%s%s%s   %s•%s %s%s%s", CYAN, BOLD, VERTICAL, RESET, RESET, GREEN, user, RESET);
            padding = 60 - len;
            for (int i = 0; i < padding; i++) printf(" ");
            printf(" %s%s%s\n", CYAN, VERTICAL, RESET);
            user = strtok(NULL, ",");
        }
    }

    printf("%s%s", CYAN, BOLD);
    print_box_bottom(width);
    // printf("%s\n", RESET);
    fflush(stdout);
}

void print_request_list(const char* requests) {
    int width = 74;  // Increased width to accommodate all columns
    
    printf("\n%s%s", YELLOW, BOLD);
    print_box_top(width);
    print_centered_line("ACCESS REQUESTS", width+1, YELLOW);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // Parse the request data
    char temp[BUFFER_SIZE * 2];
    strncpy(temp, requests, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    char *line = strtok(temp, "\n");
    int count = 0;
    int is_header = 1;
    
    while (line != NULL) {
        if (strlen(line) > 0) {
            if (strcmp(line, "No requests.") == 0) {
                print_box_line("No requests found", width, YELLOW);
                break;
            }
            
            if (is_header) {
                // Print header in bold white - adjusted width
                int header_len = strlen(line);
                printf("%s%s%s %s%s%s%s", YELLOW, BOLD, VERTICAL, RESET, BOLD, line, RESET);
                int padding = width - header_len - 2;
                for (int i = 0; i < padding; i++) printf(" ");
                printf(" %s%s%s\n", YELLOW, VERTICAL, RESET);
                is_header = 0;
            } else {
                // Parse each request line to apply colors
                int id;
                char type[10], file[20], requester[20], owner[20], status[10];
                if (sscanf(line, "%d %s %s %s %s %s", &id, type, file, requester, owner, status) == 6) {
                    char formatted[256];
                    const char *status_color = RESET;
                    
                    // Color code by status
                    if (strcmp(status, "PENDING") == 0) {
                        status_color = YELLOW;
                    } else if (strcmp(status, "APPROVED") == 0) {
                        status_color = GREEN;
                    } else if (strcmp(status, "DENIED") == 0) {
                        status_color = RED;
                    }
                    
                    // Color code by type
                    const char *type_color = (strcmp(type, "WRITE") == 0) ? MAGENTA : CYAN;
                    
                    snprintf(formatted, sizeof(formatted), "%s%2d%s  %s%-6s%s %-16.16s %-15.15s %-15.15s %s%-8s%s",
                             BOLD, id, RESET,
                             type_color, type, RESET,
                             file, requester, owner,
                             status_color, status, RESET);
                    
                    int content_len = 3 + 1 + 6 + 1 + 16 + 1 + 15 + 1 + 15 + 1 + 8;  // Calculate actual content length
                    printf("%s%s%s %s%s%s", YELLOW, BOLD, VERTICAL, RESET, formatted, RESET);
                    int padding = width - content_len - 1 - 1;
                    for (int i = 0; i < padding; i++) printf(" ");
                    printf(" %s%s%s\n", YELLOW, VERTICAL, RESET);
                } else {
                    // Fallback for lines that don't match format
                    int line_len = strlen(line);
                    printf("%s%s%s %s%s%s", YELLOW, BOLD, VERTICAL, RESET, line, RESET);
                    int padding = width - line_len - 1;
                    for (int i = 0; i < padding; i++) printf(" ");
                    printf(" %s%s%s\n", YELLOW, VERTICAL, RESET);
                }
                count++;
            }
        }
        line = strtok(NULL, "\n");
    }
    
    if (count == 0 && is_header == 0) {
        print_box_line("No requests found", width, YELLOW);
    }
    
    printf("%s%s", YELLOW, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
    fflush(stdout);
}

void print_folder_contents(const char* contents, const char* foldername) {
    int width = 62;
    
    printf("\n%s%s", MAGENTA, BOLD);
    print_box_top(width);
    
    char title[100];
    snprintf(title, sizeof(title), "FOLDER: %s", foldername);
    print_centered_line(title, width+1, MAGENTA);
    print_box_middle(width);
    // printf("%s\n", RESET);
    
    // Parse and display contents line by line
    char temp[BUFFER_SIZE * 4];
    strncpy(temp, contents, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    char *line = strtok(temp, "\n");
    int count = 0;
    while (line != NULL) {
        if (strlen(line) > 0 && strncmp(line, "ERR_", 4) != 0 && strcmp(line, "Folder is empty.") != 0) {
            // Wrap long lines instead of truncating
            int line_len = strlen(line);
            int pos = 0;
            while (pos < line_len) {
                char display_line[61];
                int remaining = line_len - pos;
                int to_copy = (remaining > 60) ? 60 : remaining;
                strncpy(display_line, line + pos, to_copy);
                display_line[to_copy] = '\0';
                printf("%s%s%s %s%-60s %s%s\n", MAGENTA, BOLD, VERTICAL, RESET, display_line, MAGENTA, VERTICAL);
                pos += to_copy;
            }
            count++;
        }
        line = strtok(NULL, "\n");
    }
    
    if (count == 0 || strstr(contents, "Folder is empty.")) {
        print_box_line("Folder is empty", width, MAGENTA);
    }
    
    printf("%s%s", MAGENTA, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
    fflush(stdout);
}

void print_trash_bin(const char* contents) {
    int width = 62;
    
    printf("\n%s%s", RED, BOLD);
    print_box_top(width);
    print_centered_line("RECYCLE BIN", width+1, RED);
    print_box_middle(width);
    
    // Parse and display contents line by line
    char temp[BUFFER_SIZE * 4];
    strncpy(temp, contents, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    char *line = strtok(temp, "\n");
    int count = 0;
    while (line != NULL) {
        if (strlen(line) > 0 && strncmp(line, "Trash is empty", 14) != 0) {
            // Wrap long lines instead of truncating
            int line_len = strlen(line);
            int pos = 0;
            while (pos < line_len) {
                char display_line[61];
                int remaining = line_len - pos;
                int to_copy = (remaining > 60) ? 60 : remaining;
                strncpy(display_line, line + pos, to_copy);
                display_line[to_copy] = '\0';
                printf("%s%s%s %s%-60s %s%s\n", RED, BOLD, VERTICAL, RESET, display_line, RED, VERTICAL);
                pos += to_copy;
            }
            count++;
        }
        line = strtok(NULL, "\n");
    }
    
    if (count == 0 || strstr(contents, "Trash is empty")) {
        print_box_line("Trash is empty", width, RED);
    }
    
    printf("%s%s", RED, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
    fflush(stdout);
}

void print_file_content(const char* content, const char* filename) {
    int width = 80;
    
    printf("\n%s%s", CYAN, BOLD);
    print_box_top(width);
    
    char title[100];
    snprintf(title, sizeof(title), "FILE: %s", filename);
    print_centered_line(title, width+1, CYAN);
    print_box_middle(width);
    
    // Display content line by line with wrapping
    char temp[BUFFER_SIZE * 4];
    strncpy(temp, content, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';
    
    if (strlen(temp) == 0) {
        print_box_line("(empty file)", width, CYAN);
    } else {
        char *line = strtok(temp, "\n");
        while (line != NULL) {
            // Wrap long lines instead of truncating
            int line_len = strlen(line);
            int pos = 0;
            while (pos < line_len) {
                char display_line[79];
                int remaining = line_len - pos;
                int to_copy = (remaining > 78) ? 78 : remaining;
                strncpy(display_line, line + pos, to_copy);
                display_line[to_copy] = '\0';
                printf("%s%s%s %s%-78s %s%s\n", CYAN, BOLD, VERTICAL, RESET, display_line, CYAN, VERTICAL);
                pos += to_copy;
            }
            line = strtok(NULL, "\n");
        }
    }
    
    printf("%s%s", CYAN, BOLD);
    print_box_bottom(width);
    printf("%s\n", RESET);
    fflush(stdout);
}

// --- NEW FUNCTION ---
// Handles the direct connection to the Storage Server
void handle_ss_connection(const char* ss_ip, int ss_port, const char* full_command) {
    int ss_sock = connect_to_server(ss_ip, ss_port);
    
    // 1. Send the original command to the SS
    if (write(ss_sock, full_command, strlen(full_command)) < 0) {
        die("ERROR writing to SS");
    }

    char buffer[BUFFER_SIZE];
    int read_size;

    // --- READ Logic ---
    if (strncmp(full_command, "READ", 4) == 0) {
        // Extract filename
        char filename[256];
        sscanf(full_command, "READ %s", filename);
        
        // Read all content first
        char content[BUFFER_SIZE * 4];
        bzero(content, sizeof(content));
        int total = 0;
        
        while ((read_size = read(ss_sock, buffer, BUFFER_SIZE - 1)) > 0) {
            buffer[read_size] = '\0';
            if (total + read_size < sizeof(content)) {
                strcat(content, buffer);
                total += read_size;
            }
        }
        
        // Print in a box
        print_file_content(content, filename);
    }
    
    // --- STREAM Logic ---
    else if (strncmp(full_command, "STREAM", 6) == 0) {
        printf("\n%s%s--- STREAMING FILE ---%s\n", YELLOW, BOLD, RESET);
        // For streaming, read one character at a time to display word-by-word
        char ch;
        while (read(ss_sock, &ch, 1) > 0) {
            printf("%c", ch);
            fflush(stdout); // Force immediate display
        }
        printf("\n%s%s--- END OF STREAM ---%s\n\n", YELLOW, BOLD, RESET);
    }
    
    // --- WRITE Logic ---
    else if (strncmp(full_command, "WRITE", 5) == 0) {
        // Wait for the lock ACK
        bzero(buffer, BUFFER_SIZE);
        if (read(ss_sock, buffer, BUFFER_SIZE) < 0) {
            die("ERROR reading from SS");
        }
        
        if (strncmp(buffer, "ACK_WRITE_LOCKED", 16) != 0) {
            printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            close(ss_sock);
            return;
        }
        
        printf("%s[WRITE MODE]%s Sentence locked. Enter updates (<word_index> <content>).\n", GREEN, RESET);
        printf("%s[WRITE MODE]%s Type 'ETIRW' to finish and save.\n", GREEN, RESET);
        
        // Enter dedicated WRITE loop
        while (1) {
            printf("%sWRITE >%s ", YELLOW, RESET);
            bzero(buffer, BUFFER_SIZE);
            fgets(buffer, BUFFER_SIZE, stdin);
            
            // Send to SS
            if (write(ss_sock, buffer, strlen(buffer)) < 0) {
                die("ERROR writing to SS during write");
            }
            
            if (strncmp(buffer, "ETIRW", 5) == 0) {
                break; // Exit loop
            }
        }
        
        // Wait for final ACK from SS
        bzero(buffer, BUFFER_SIZE);
        read(ss_sock, buffer, BUFFER_SIZE);
        
        if (strncmp(buffer, "ACK_WRITE_SUCCESS", 17) == 0) {
            printf("%s[SUCCESS]%s File saved successfully!\n", GREEN, RESET);
        } else {
            printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
        }
    }
    
    // --- UNDO Logic ---
    else if (strncmp(full_command, "UNDO", 4) == 0) { 
        // Wait for the ACK from the SS
        bzero(buffer, BUFFER_SIZE);
        if (read(ss_sock, buffer, BUFFER_SIZE) < 0) {
            die("ERROR reading from SS");
        }
        
        if (strncmp(buffer, "ACK_UNDO_SUCCESS", 16) == 0) {
            printf("%s[SUCCESS]%s Undo operation completed!\n", GREEN, RESET);
        } else {
            printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
        }
    }

    // --- CHECKPOINT Logic ---
    else if (strncmp(full_command, "CHECKPOINT", 10) == 0) {
        bzero(buffer, BUFFER_SIZE);
        if (read(ss_sock, buffer, BUFFER_SIZE) > 0) {
            if (strncmp(buffer, "ACK_CHECKPOINT", 14) == 0) {
                printf("%s[SUCCESS]%s Checkpoint saved.\n", GREEN, RESET);
            } else {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            }
        }
    }
    else if (strncmp(full_command, "VIEWCHECKPOINT", 14) == 0) {
        // Extract filename and tag for title
        char filename[256], tag[128];
        bzero(filename, sizeof(filename)); bzero(tag, sizeof(tag));
        sscanf(full_command, "VIEWCHECKPOINT %255s %127s", filename, tag);
        char content[BUFFER_SIZE * 4]; bzero(content, sizeof(content));
        int total = 0;
        while ((read_size = read(ss_sock, buffer, BUFFER_SIZE - 1)) > 0) {
            buffer[read_size] = '\0';
            if (total + read_size < (int)sizeof(content)) { strcat(content, buffer); total += read_size; }
        }
        char title[400]; snprintf(title, sizeof(title), "%s@%s", filename, tag);
        print_file_content(content, title);
    }
    else if (strncmp(full_command, "LISTCHECKPOINTS", 15) == 0) {
        // Read list and print box
        char listbuf[BUFFER_SIZE * 2]; bzero(listbuf, sizeof(listbuf));
        int total=0; while ((read_size = read(ss_sock, buffer, BUFFER_SIZE - 1)) > 0) { buffer[read_size]='\0'; if (total+read_size < (int)sizeof(listbuf)) { strcat(listbuf, buffer); total+=read_size; } }
        int width = 62; printf("\n%s%s", GREEN, BOLD); print_box_top(width); print_centered_line("CHECKPOINTS", width+1, GREEN); print_box_middle(width);
        // print each line
        char temp[BUFFER_SIZE*2]; strncpy(temp, listbuf, sizeof(temp)-1); temp[sizeof(temp)-1]='\0';
        char *line = strtok(temp, "\n"); int count=0;
        while (line) { if (strlen(line)>0) { printf("%s%s%s %s%-60s %s%s\n", GREEN, BOLD, VERTICAL, RESET, line, GREEN, VERTICAL); count++; } line = strtok(NULL, "\n"); }
        if (count==0) { print_box_line("(no checkpoints)", width, GREEN); }
        printf("%s%s", GREEN, BOLD); print_box_bottom(width); printf("%s\n", RESET);
    }
    else if (strncmp(full_command, "REVERT", 6) == 0) {
        bzero(buffer, BUFFER_SIZE);
        if (read(ss_sock, buffer, BUFFER_SIZE) > 0) {
            if (strncmp(buffer, "ACK_REVERT", 10) == 0) {
                printf("%s[SUCCESS]%s Reverted to checkpoint.\n", GREEN, RESET);
            } else {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            }
        }
    }

    close(ss_sock);
}




int main(int argc, char *argv[]) {
    int nm_sock;
    char buffer[BUFFER_SIZE];
    char username[100];

    // Show welcome banner ---
    print_welcome_banner();
    
    // Get username ---
    printf("Give us your username ▄︻デ══━一 ");
    fgets(username, 100, stdin);
    username[strcspn(username, "\n")] = 0; // Remove trailing newline
    
    // --- Step 3: Connect to Name Server and register ---
    printf("%s[Client]%s Connecting to Name Server at %s:%d...\n", BLUE, RESET, NM_IP, NM_PORT);
    nm_sock = connect_to_server(NM_IP, NM_PORT);
    
    // Send registration message with username
    char reg_msg[BUFFER_SIZE];
    snprintf(reg_msg, sizeof(reg_msg), "REG_CLIENT %s\n", username);
    if (write(nm_sock, reg_msg, strlen(reg_msg)) < 0) {
        die("ERROR writing to NM");
    }
    
    // Wait for acknowledgment
    bzero(buffer, BUFFER_SIZE);
    int read_size = read(nm_sock, buffer, BUFFER_SIZE - 1);
    if (read_size < 0) {
        die("ERROR reading from NM");
    }
    buffer[read_size] = '\0';
    
    if (strncmp(buffer, "ACK_REG", 7) != 0) {
        // Strip newline for cleaner display
        char *newline = strchr(buffer, '\n');
        if (newline) *newline = '\0';
        
        // Provide user-friendly error messages
        if (strcmp(buffer, "ERR_USERNAME_IN_USE") == 0) {
            printf("%s[ERROR]%s Username '%s%s%s' is already in use. Please try a different username.\n", 
                   RED, RESET, CYAN, username, RESET);
        } else {
            printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
        }
        close(nm_sock);
        return 1;
    }
    printf("%s[SUCCESS]%s Connected as '%s%s%s'\n\n", GREEN, RESET, CYAN, username, RESET);

    // --- Step 4: Show help on first login ---
    print_help();

    // --- Step 5: Main command loop ---
    printf("Type '%sexit%s' to quit or '%shelp%s' for command list.\n\n", RED, RESET, CYAN, RESET);
    char user_input[BUFFER_SIZE];

    while (1) {
        // Create prompt with colors
        char prompt[256];
        snprintf(prompt, sizeof(prompt), "%s%s%s%s > ", CYAN, BOLD, username, RESET);
        
        // Use readline for better input handling (arrow keys, history, etc.)
        char *line = readline(prompt);
        
        // Check for EOF (Ctrl+D)
        if (line == NULL) {
            printf("\n");
            break;
        }
        
        // Skip empty lines
        if (strlen(line) == 0) {
            free(line);
            continue;
        }
        
        // Add to history for up/down arrow navigation
        add_history(line);
        
        // Copy to user_input buffer and add newline for server
        bzero(user_input, BUFFER_SIZE);
        snprintf(user_input, BUFFER_SIZE, "%s\n", line);
        
        // Free readline buffer
        free(line);

        if (strncmp(user_input, "exit", 4) == 0) {
            break;
        }
        
        if (strncmp(user_input, "help", 4) == 0) {
            print_help();
            continue;
        }
        
        if (strncmp(user_input, "man ", 4) == 0) {
            char cmd[100];
            if (sscanf(user_input, "man %s", cmd) == 1) {
                print_man_page(cmd);
            } else {
                printf("%s[ERROR]%s Usage: man <command>\n", RED, RESET);
            }
            continue;
        }

        // Send command to NM
        if (write(nm_sock, user_input, strlen(user_input)) < 0) {
            die("ERROR writing to NM");
        }

        // Read NM's response
        bzero(buffer, BUFFER_SIZE);
        read_size = read(nm_sock, buffer, BUFFER_SIZE - 1);
        if (read_size < 0) {
            die("ERROR reading from NM");
        }
        buffer[read_size] = '\0';

        // --- Check for SS redirect ---
        char ss_ip[100];
        int ss_port;
        
        if (sscanf(buffer, "ACK_READ %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_STREAM %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_WRITE %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_UNDO %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_CHECKPOINT %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_VIEWCHECKPOINT %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_LISTCHECKPOINTS %s %d", ss_ip, &ss_port) == 2 ||
            sscanf(buffer, "ACK_REVERT %s %d", ss_ip, &ss_port) == 2)
        {
            // Print status messages from main, NOT from the handler
            printf("%s[Client]%s Connecting to Storage Server at %s:%d...\n", BLUE, RESET, ss_ip, ss_port);
            
            handle_ss_connection(ss_ip, ss_port, user_input);
            
            printf("%s[Client]%s Disconnected from Storage Server.\n", BLUE, RESET);
            continue; // Skip remaining checks after handling SS connection
        }
        // --- Check for VIEW command (exact match or VIEW followed by space/newline) ---
        else if (strncmp(user_input, "VIEW\n", 5) == 0 || strncmp(user_input, "VIEW ", 5) == 0) {
            print_file_list(buffer);
        }
        // --- Check for LIST command (exact match or LIST followed by space/newline) ---
        else if (strncmp(user_input, "LIST\n", 5) == 0 || strncmp(user_input, "LIST ", 5) == 0) {
            print_user_list(buffer, username);
        }
        // --- Check for VIEWFOLDER command ---
        else if (strncmp(user_input, "VIEWFOLDER", 10) == 0) {
            char foldername[256];
            sscanf(user_input, "VIEWFOLDER %s", foldername);
            
            if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                print_folder_contents(buffer, foldername);
            }
        }
        // --- Check for VIEWTRASH command ---
        else if (strncmp(user_input, "VIEWTRASH\n", 10) == 0 || strncmp(user_input, "VIEWTRASH ", 10) == 0) {
            if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                print_trash_bin(buffer);
            }
        }
        // --- Check for TRASH command ---
        else if (strncmp(user_input, "TRASH ", 6) == 0) {
            if (strncmp(buffer, "ACK_TRASHED", 11) == 0) {
                printf("%s[SUCCESS]%s File moved to trash.\n", GREEN, RESET);
            } else if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                printf("%s", buffer);
            }
        }
        // --- Check for RESTORE command ---
        else if (strncmp(user_input, "RESTORE ", 8) == 0) {
            if (strncmp(buffer, "ACK_RESTORED", 12) == 0) {
                printf("%s[SUCCESS]%s File restored from trash.\n", GREEN, RESET);
            } else if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                printf("%s", buffer);
            }
        }
        // --- Check for EMPTYTRASH command ---
        else if (strncmp(user_input, "EMPTYTRASH\n", 11) == 0 || strncmp(user_input, "EMPTYTRASH ", 11) == 0) {
            if (strncmp(buffer, "ACK_EMPTYTRASH", 14) == 0) {
                printf("%s[SUCCESS]%s %s\n", GREEN, RESET, buffer);
            } else if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                printf("%s", buffer);
            }
        }
        // --- Check for INFO command response ---
        else if (strncmp(user_input, "INFO", 4) == 0) {
            if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                print_file_info(buffer);
            }
        }
        // --- Check for LISTREQ command response ---
        else if (strncmp(user_input, "LISTREQ\n", 8) == 0 || strncmp(user_input, "LISTREQ ", 8) == 0) {
            if (strncmp(buffer, "ERR_", 4) == 0) {
                printf("%s[ERROR]%s %s\n", RED, RESET, buffer);
            } else {
                print_request_list(buffer);
            }
        }
        // --- Success messages ---
        else if (strncmp(buffer, "ACK_", 4) == 0) {
            printf("%s[SUCCESS]%s %s", GREEN, RESET, buffer);
        }
        // --- Check for shutdown message from server ---
        else if (strncmp(buffer, "SHUTDOWN", 8) == 0) {
            printf("\n%s[NOTICE]%s Name Server is shutting down.\n", YELLOW, RESET);
            printf("%s[Client]%s Disconnecting...\n", BLUE, RESET);
            close(nm_sock);
            printf("%sGoodbye!%s\n\n", YELLOW, RESET);
            exit(0);
        }
        // --- Error messages ---
        else if (strncmp(buffer, "ERR_", 4) == 0) {
            printf("%s[ERROR]%s %s", RED, RESET, buffer);
        }
        // --- Everything else ---
        else {
            printf("%s", buffer);
        }
    }

    close(nm_sock);
    printf("\n%s[Client]%s Disconnected from Name Server.\n", BLUE, RESET);
    printf("%sGoodbye!%s\n\n", YELLOW, RESET);
    return 0;
}
