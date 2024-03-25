#include <ctype.h>
#include <getopt.h>
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

#define fori(i, n) for (int i = 0; i < n; i++)
#define is_str_equal(str1, str2) (strcmp(str1, str2) == 0)

typedef struct {
	char *key;
	char *value;
	time_t ttl;
} KeyValue;

typedef struct Server {
	int port;
	struct Server *replicaof;
} Server;

KeyValue key_values[KEYS_SIZE];
int key_values_size = 0;

Server server = {6379};

time_t currentMillis() {
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

int get_key_index(char *key) {
	fori(i, key_values_size) {
		if (is_str_equal(key_values[i].key, key))
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
	char buffer[1024] = {0};
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
	send(client_fd, "+OK\r\n", 5, 0);
}

void get(int client_fd, char *key) {
	if (key == NULL) {
		send(client_fd, "-Missing key argument\r\n", 6, 0);
		return;
	}
	int index = get_key_index(key);
	char *value = key_values[index].value;
	time_t ttl = key_values[index].ttl;
	if (index == -1 || ttl != 0 && currentMillis() > ttl) {
		send(client_fd, "$-1\r\n", 5, 0);
	} else {
		char buffer[1024] = {0};
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
			message = "3*\r\n$11\r\nrole:master\r\n"
					  "$14\r\nmaster_replid:"
					  "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
					  "$20\r\nmaster_repl_offset:0\r\n";
		} else {
			message = "3*\r\n$10\r\nrole:slave\r\n"
					  "$14\r\nmaster_replid:"
					  "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
					  "$20\r\nmaster_repl_offset:0\r\n";
		}
		send(client_fd, message, strlen(message), 0);
	} else {
		send(client_fd, "-Unknown argument\r\n", 6, 0);
	}
}

void evaluate_commands(char **commands, int num_args, int client_fd) {
	fori(i, num_args) {
		char *command = to_lowercase(commands[i]);
		if is_str_equal (command, "ping") {
			send(client_fd, "+PONG\r\n", 7, 0);
		} else if is_str_equal (command, "echo") {
			echo(client_fd, commands[i + 1]);
		} else if is_str_equal (command, "set") {
			set(client_fd, commands[i + 1], commands[i + 2], commands[i + 4]);
		} else if is_str_equal (command, "get") {
			get(client_fd, commands[i + 1]);
		} else if is_str_equal (command, "info") {
			info(client_fd, commands[i + 1]);
		}
	}
}

void *handle_client(void *args) {
	int client_fd = *(int *)args;
	printf("Client connected\n");
	while (1) {
		char buffer[1024] = {0};
		int valread = read(client_fd, buffer, 1024);
		if (valread == 0) {
			break;
		}

		char *data = strtok(buffer, "\r\n");
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
				evaluate_commands(commands, num_args, client_fd);
				free(commands);
			}
		} while ((data = strtok(NULL, "\r\n")) != NULL);
	}
	close(client_fd);
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
		if (opt == 'p')
			server.port = atoi(optarg);
		else if (opt == 'r') {
			server.replicaof = malloc(sizeof(Server));
		} else {
			printf("Usage: %s [--port PORT] [--replicaof HOST PORT]\n",
				   argv[0]);
			return 1;
		}
	}

	struct sockaddr_in serv_addr = {
		.sin_family = AF_INET,
		.sin_port = htons(server.port),
		.sin_addr = {htonl(INADDR_ANY)},
	};

	if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) !=
		0) {
		perror("Bind failed");
		return 1;
	}

	int connection_backlog = 5;
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

		pthread_t thread;
		if (pthread_create(&thread, NULL, handle_client, &client_fd) != 0) {
			perror("pthread_create");
			return 1;
		}

		if (pthread_detach(thread) != 0) {
			perror("pthread_detach");
			return 1;
		}
	}

	fori(i, key_values_size) {
		free(key_values[i].key);
		free(key_values[i].value);
	}

	close(server_fd);
	return 0;
}
