#include <ctype.h>
#include <getopt.h>
#include <math.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define KEYS_SIZE 100
#define BUFFER_SIZE 1024
#define SMALL_BUFFER_SIZE 128

#define fori(i, n) for (int i = 0; i < n; i++)
#define is_str_equal(str1, str2) (strcmp(str1, str2) == 0)

typedef struct {
	char *key;
	char *value;
	time_t ttl;
} KeyValue;

typedef struct Server {
	int port;
	int fd;
	char *host;
	struct Server *replicaof;
} Server;

KeyValue key_values[KEYS_SIZE];
int key_values_size = 0;

int replicas_fd[10] = {0};
int replicas_size = 0;

int ack = 0;
Server server = {.port = 6379, .host = "localhost"};

time_t currentMillis() {
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

int get_key_index(char *key) {
	fori(i, key_values_size) {
		if is_str_equal (key_values[i].key, key)
			return i;
	}
	return -1;
}

char *parse_string(char *data) {
	int len = atoi(data + 1);
	return strtok(NULL, "\r\n");
}

char *to_lowercase(char *str) {
	for (int i = 0; str[i]; i++) {
		str[i] = tolower((int)str[i]);
	}
	return str;
}

char *get_key_value(char *key) {
	int index = get_key_index(key);
	return index == -1 ? NULL : key_values[index].value;
}

void echo(int client_fd, char *echo) {
	if (echo == NULL) {
		send(client_fd, "-Missing argument\r\n", 6, 0);
		return;
	}
	char buffer[BUFFER_SIZE] = {0};
	int len = sprintf(buffer, "$%lu\r\n%s\r\n", strlen(echo), echo);
	send(client_fd, buffer, len, 0);
}

void set(int client_fd, char *key, char *value, char *ttl) {
	if (key == NULL || value == NULL) {
		send(client_fd, "-Missing arguments\r\n", 6, 0);
		return;
	}
	int index = get_key_index(key);
	time_t key_ttl = ttl == NULL ? 0 : atoi(ttl) + currentMillis();
	if (index == -1) {
		key_values[key_values_size].key = strdup(key);
		key_values[key_values_size].value = strdup(value);
		key_values[key_values_size].ttl = key_ttl;
		key_values_size++;
	} else {
		free(key_values[index].value);
		key_values[index].value = strdup(value);
		key_values[index].ttl = key_ttl;
	}

	if (server.replicaof != NULL && server.replicaof->fd == client_fd) {
		printf("Replicated SET %s %s\n", key, value);
		return;
	}

	send(client_fd, "+OK\r\n", 5, 0);
}

void get(int client_fd, char *key) {
	if (key == NULL) {
		send(client_fd, "-Missing key argument\r\n", 6, 0);
		return;
	}
	int index = get_key_index(key);
	time_t ttl = key_values[index].ttl;
	if (index == -1 || ttl > 0 && currentMillis() > ttl) {
		send(client_fd, "$-1\r\n", 5, 0);
	} else {
		char buffer[BUFFER_SIZE] = {0};
		char *value = key_values[index].value;
		int len = sprintf(buffer, "$%lu\r\n%s\r\n", strlen(value), value);
		send(client_fd, buffer, len, 0);
	}
}

void info(int client_fd, char *info) {
	if (info == NULL) {
		send(client_fd, "-Missing argument\r\n", 6, 0);
		return;
	}
	if is_str_equal (info, "replication") {
		char *message;
		if (server.replicaof == NULL) {
			message = "$87\r\nrole:master\n"
					  "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n"
					  "master_repl_offset:0\r\n";
		} else {
			message = "$86\r\nrole:slave\n"
					  "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n"
					  "master_repl_offset:0\r\n";
		}
		send(client_fd, message, strlen(message), 0);
	} else {
		send(client_fd, "-Unknown argument\r\n", 6, 0);
	}
}

void psync(int client_fd) {
	send(client_fd,
		 "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n", 56, 0);

	char empty_hex_rdb[] =
		"524544495330303131fa0972656469732d76657205372e322e30fa0a726564"
		"69732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656d"
		"c2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

	char binary_rdb[BUFFER_SIZE];
	int i = 0, j = 0;
	while (empty_hex_rdb[i]) {
		char byte[3] = {empty_hex_rdb[i], empty_hex_rdb[i + 1], '\0'};
		binary_rdb[j++] = (char)strtol(byte, NULL, 16);
		i += 2;
	}

	char buffer[BUFFER_SIZE] = {0};
	int len = snprintf(buffer, BUFFER_SIZE, "$%lu\r\n%s", strlen(binary_rdb),
					   binary_rdb);
	send(client_fd, buffer, len, 0);
}

void ping(int client_fd) {
	if (server.replicaof != NULL && server.replicaof->fd == client_fd) {
		printf("Replica pinged\n");
		return;
	}
	send(client_fd, "+PONG\r\n", 7, 0);
}

void replconf(int client_fd, char *key) {
	if is_str_equal (key, "getack") {
		char buffer[BUFFER_SIZE] = {0};
		int digits = floor(log10(ack + 1)) + 1;
		int len = sprintf(buffer,
						  "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
						  digits, ack);
		send(client_fd, buffer, len, 0);
		return;
	}

	send(client_fd, "+OK\r\n", 5, 0);
}

// Returns -> 0: success, 1: resend to replicas
int evaluate_commands(char **commands, int num_args, int client_fd) {
	int action = 0;
	fori(i, num_args) {
		char *command = to_lowercase(commands[i]);
		char *key = commands[i + 1];
		if is_str_equal (command, "ping") {
			ping(client_fd);
			ack += 14;
		} else if is_str_equal (command, "echo") {
			echo(client_fd, key);
		} else if is_str_equal (command, "set") {
			action = 1;
			char *value = commands[i + 2];
			char *ttl = i + 4 < num_args ? commands[i + 4] : NULL;
			set(client_fd, key, value, ttl);
			// *n\r\n$3\r\nset\r\n$n\r\nkey\r\n$n\r\nvalue\r\n
			ack += 25 + strlen(key) + strlen(value);
			// $2\r\npx\r\n$n\r\nttl\r\n
			if (ttl != NULL)
				ack += 14 + strlen(ttl);
		} else if is_str_equal (command, "get") {
			get(client_fd, key);
		} else if is_str_equal (command, "info") {
			key = to_lowercase(key);
			info(client_fd, key);
		} else if is_str_equal (command, "replconf") {
			key = to_lowercase(key);
			replconf(client_fd, key);
			ack += 37;
		} else if is_str_equal (command, "psync") {
			psync(client_fd);
			replicas_fd[replicas_size++] = client_fd;
		}
	}
	return action;
}

void *handle_client(void *args) {
	int client_fd = *(int *)args;
	printf("Client connected %d\n", client_fd);
	while (1) {
		char buffer[BUFFER_SIZE] = {0};
		if (read(client_fd, buffer, BUFFER_SIZE) <= 0)
			break;

		char *data = strtok(strdup(buffer), "\r\n");
		do {
			if (data[0] == '*') {
				int num_args = atoi(data + 1);
				char **commands = malloc(num_args * sizeof(char *));
				fori(i, num_args) {
					data = strtok(NULL, "\r\n");
					if (data[0] == '$') {
						commands[i] = parse_string(data);
						printf("str: %s\n", commands[i]);
					}
				}
				int res = evaluate_commands(commands, num_args, client_fd);
				if (res == 1) {
					fori(i, replicas_size) {
						if (replicas_fd[i] != client_fd) {
							printf("Sending to replica %d\n", replicas_fd[i]);
							send(replicas_fd[i], buffer, strlen(buffer), 0);
						}
					}
				}
				free(commands);
			}
		} while ((data = strtok(NULL, "\r\n")) != NULL);
	}

	printf("Client disconnected %d\n", client_fd);
	close(client_fd);
	return NULL;
}

int send_repl_hs(char *message, char *expected_response, int match_length) {
	if (match_length <= 0)
		match_length = SMALL_BUFFER_SIZE;

	if (send(server.replicaof->fd, message, strlen(message), 0) == -1) {
		perror("Error during SENDING replication handshake");
		return 1;
	}

	char buffer[SMALL_BUFFER_SIZE] = {0};
	if (recv(server.replicaof->fd, buffer, SMALL_BUFFER_SIZE, 0) == -1 ||
		strncmp(buffer, expected_response, match_length) != 0) {
		perror("Error during RECEIVING replication handshake");
		printf("Expected: %s\n", expected_response);
		printf("Received: %s\n", buffer);
		return 1;
	}
	return 0;
}

int send_to_thread(void *func, void *args) {
	pthread_t thread;
	if (pthread_create(&thread, NULL, func, args) != 0) {
		perror("pthread_create");
		return 1;
	}

	if (pthread_detach(thread) != 0) {
		perror("pthread_detach");
		return 1;
	}
	return 0;
}

int client_to_thread(int *client_fd) {
	return send_to_thread(handle_client, client_fd);
}

void *replicate() {
	struct sockaddr_in repl_addr = {
		.sin_family = AF_INET,
		.sin_port = htons(server.replicaof->port),
		.sin_addr = {htonl(INADDR_ANY)},
	};

	server.replicaof->fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server.replicaof->fd == -1) {
		perror("Socket creation failed");
		return NULL;
	}

	if (connect(server.replicaof->fd, (struct sockaddr *)&repl_addr,
				sizeof(repl_addr)) != 0) {
		perror("Connection to replica failed");
		return NULL;
	}

	printf("Connected to master on http://%s:%d\n", server.replicaof->host,
		   server.replicaof->port);

	int result;
	result = send_repl_hs("*1\r\n$4\r\nping\r\n", "+PONG\r\n", 0);
	if (result != 0) {
		perror("Error during PING");
		return NULL;
	}

	result = send_repl_hs("*3\r\n$8\r\nREPLCONF\r\n$14\r\n"
						  "listening-port\r\n$4\r\n6380\r\n",
						  "+OK\r\n", 0);
	if (result != 0) {
		perror("Error during REPLCONF listening-port\n");
		return NULL;
	}

	result = send_repl_hs("*3\r\n$8\r\nREPLCONF\r\n"
						  "$4\r\ncapa\r\n$6\r\npsync2\r\n",
						  "+OK\r\n", 0);
	if (result != 0) {
		perror("Error during REPLCONF ncapa\n");
		return NULL;
	}

	result = send_repl_hs("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
						  "+FULLRESYNC", 11);
	if (result != 0) {
		perror("Error during PSYNC\n");
		return NULL;
	}

	char buffer[BUFFER_SIZE] = {0};
	if (recv(server.replicaof->fd, buffer, BUFFER_SIZE, 0) == -1) {
		perror("Failed to receive data");
		return NULL;
	}
	printf("Received RDB.\n");

	result = client_to_thread(&server.replicaof->fd);
	if (result != 0) {
		perror("Error during client_to_thread of replica\n");
		return NULL;
	}
	printf("Replication handshake successful\n");
	return NULL;
}

int main(int argc, char const *argv[]) {
	// Disable output buffering
	setbuf(stdout, NULL);

	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1) {
		perror("Socket creation failed");
		return 1;
	}

	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
		0) {
		perror("SO_REUSEADDR failed");
		return 1;
	}

	const struct option long_options[] = {
		{"port", required_argument, NULL, 'p'},
		{"replicaof", required_argument, NULL, 'r'},
	};

	int opt;
	while ((opt = getopt_long(argc, (char *const *)argv, "p:r", long_options,
							  NULL)) != -1) {
		if (opt == 'p') {
			server.port = atoi(optarg);
		} else if (opt == 'r') {
			// Should check for errors here
			server.replicaof = malloc(sizeof(Server));
			server.replicaof->host = optarg;
			server.replicaof->port = atoi(argv[optind]);
		} else {
			printf("Usage: %s [--port PORT] [--replicaof HOST PORT]\n",
				   argv[0]);
			return 1;
		}
	}

	if (server.replicaof != NULL && server.replicaof->port == server.port) {
		fprintf(stderr,
				"Error: replicaof port cannot be the same as server port\n");
		return 1;
	}

	struct sockaddr_in serv_addr = {
		.sin_family = AF_INET,
		.sin_port = htons(server.port),
		.sin_addr = {htonl(INADDR_ANY)},
	};

	if (server.replicaof != NULL) {
		if (send_to_thread(replicate, NULL) != 0) {
			perror("Error during replicate\n");
			return 1;
		}
	}

	if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) !=
		0) {
		perror("Bind failed");
		return 1;
	}

	int connection_backlog = 10;
	if (listen(server_fd, connection_backlog) != 0) {
		perror("Listen failed");
		return 1;
	}

	printf("Waiting for a client to connect on http://localhost:%d\n",
		   server.port);
	struct sockaddr_in client_addr;
	socklen_t client_addr_len = sizeof(client_addr);
	for (;;) {
		int client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
							   &client_addr_len);
		if (client_fd == -1) {
			perror("Accept failed");
			return 1;
		}

		int result = client_to_thread(&client_fd);
		if (result != 0) {
			perror("Error during client_to_thread\n");
			return 1;
		}
	}

	if (server.replicaof != NULL) {
		close(server.replicaof->fd);
		free(server.replicaof);
	}

	fori(i, key_values_size) {
		free(key_values[i].key);
		free(key_values[i].value);
	}

	close(server_fd);

	return 0;
}
