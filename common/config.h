#ifndef CONFIG_H
#define CONFIG_H

#define NM_PORT 8080
#define NM_IP "127.0.0.1"
#define BUFFER_SIZE 1024

// Fault Tolerance Configuration
#define HEARTBEAT_INTERVAL 5     // Seconds between heartbeats
#define FAILURE_TIMEOUT 15       // Seconds before marking SS as failed
#define NM_HEARTBEAT_PORT 8081   // Port for heartbeat messages

#endif
