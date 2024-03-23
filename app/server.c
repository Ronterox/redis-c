#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

void *handle_client(void *args) {
	int client_fd = *(int *)args;
	printf("Client connected\n");
	while (1) {
		char buffer[1024] = {0};
		int valread = read(client_fd, buffer, 1024);
		if (valread == 0) {
			break;
		}
		printf("Received: %s\n", buffer);
		send(client_fd, "+PONG\r\n", 7, 0);
		printf("Sent +PONG\n");
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

	// Since the tester restarts your program quite often, setting SO_REUSEADDR
	// ensures that we don't run into 'Address already in use' errors
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

	close(server_fd);
	return 0;
}
