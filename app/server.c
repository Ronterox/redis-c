#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define KEYS_SIZE 100

#define fori(i, n) for (int i = 0; i < n; i++)
#define is_str_equal(str1, str2) (strcmp(str1, str2) == 0)

typedef struct {
	char *key;
	char *value;
} KeyValue;

KeyValue key_values[KEYS_SIZE];
int key_values_size = 0;

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

void set_key_value(char *key, char *value) {
	int index = get_key_index(key);
	if (index == -1) {
		key_values[key_values_size].key = strdup(key);
		key_values[key_values_size].value = strdup(value);
		key_values_size++;
	} else {
		free(key_values[index].value);
		key_values[index].value = strdup(value);
	}
}

char *get_key_value(char *key) {
	int index = get_key_index(key);
	return index == -1 ? NULL : key_values[index].value;
}

void evaluate_commands(char **commands, int num_args, int client_fd) {
	fori(i, num_args) {
		char *command = to_lowercase(commands[i]);
		if is_str_equal (command, "ping") {
			send(client_fd, "+PONG\r\n", 7, 0);
		} else if is_str_equal (command, "echo") {
			char *echo = commands[i + 1];
			char buffer[1024] = {0};
			int len = sprintf(buffer, "$%lu\r\n%s\r\n", strlen(echo), echo);
			send(client_fd, buffer, len, 0);
		} else if is_str_equal (command, "set") {
			char *key = commands[i + 1];
			char *value = commands[i + 2];
			set_key_value(key, value);
			send(client_fd, "+OK\r\n", 5, 0);
		} else if is_str_equal (command, "get") {
			char *key = commands[i + 1];
			char *value = get_key_value(key);
			if (value == NULL) {
				send(client_fd, "$-1\r\n", 5, 0);
			} else {
				char buffer[1024] = {0};
				int len =
					sprintf(buffer, "$%lu\r\n%s\r\n", strlen(value), value);
				send(client_fd, buffer, len, 0);
			}
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
		// printf("Received: %s\n", buffer);
		char *data = strtok(buffer, "\r\n");
		do {
			switch (data[0]) {
			case '*': {
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
				break;
			}
			}
		} while ((data = strtok(NULL, "\r\n")) != NULL);
	}
	close(client_fd);
	return NULL;
}

int main() {
	// Disable output buffering
	setbuf(stdout, NULL);

	struct sockaddr_in client_addr;

	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1) {
		printf("Socket creation failed: %s...\n", strerror(errno));
		return 1;
	}

	// Since the tester restarts your program quite often, setting
	// SO_REUSEADDR ensures that we don't run into 'Address already in use'
	// errors
	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
		0) {
		printf("SO_REUSEADDR failed: %s \n", strerror(errno));
		return 1;
	}

	struct sockaddr_in serv_addr = {
		.sin_family = AF_INET,
		.sin_port = htons(6379),
		.sin_addr = {htonl(INADDR_ANY)},
	};

	if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) !=
		0) {
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}

	int connection_backlog = 5;
	if (listen(server_fd, connection_backlog) != 0) {
		printf("Listen failed: %s \n", strerror(errno));
		return 1;
	}

	printf("Waiting for a client to connect on http://localhost:6379\n");
	socklen_t client_addr_len = sizeof(client_addr);

	for (;;) {
		int client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
							   &client_addr_len);
		if (client_fd == -1) {
			printf("Accept failed: %s \n", strerror(errno));
			return 1;
		}

		pthread_t thread;
		if (pthread_create(&thread, NULL, handle_client, &client_fd) != 0) {
			printf("Failed to create thread\n");
			return 1;
		}

		if (pthread_detach(thread) != 0) {
			printf("Failed to detach thread\n");
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
