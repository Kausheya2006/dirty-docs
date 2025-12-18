CC = gcc
CFLAGS = -pthread
LDFLAGS = -pthread

# Common object file
COMMON_OBJ = common/utils.o

all: name_server storage_server client

name_server: name_server/name_server.c $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o ns name_server/name_server.c name_server/ns_utils.c $(COMMON_OBJ) $(LDFLAGS)

storage_server: storage_server/storage_server.c $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o ss storage_server/storage_server.c storage_server/ss_utils.c $(COMMON_OBJ) $(LDFLAGS)

client: client/client.c $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o user client/client.c $(COMMON_OBJ) $(LDFLAGS) -lreadline

common/utils.o: common/utils.c common/utils.h common/config.h
	$(CC) $(CFLAGS) -c -o common/utils.o common/utils.c

clean:
	rm -f ns ss user common/utils.o