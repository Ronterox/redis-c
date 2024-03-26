#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char const *argv[]) {
	int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (sock_fd == -1) {
		perror("Socket creation failed");
		return 1;
	}

	struct sockaddr_in server_addr = {
		.sin_family = AF_INET,
		.sin_port = htons(6379),
		.sin_addr = {htonl(INADDR_ANY)},
	};

	if (connect(sock_fd, (struct sockaddr *)&server_addr,
				sizeof(server_addr)) != 0) {
		perror("Connection failed");
		return 1;
	}

	char message[1024];
	int len = sprintf(message, "*%d\r\n", argc - 1);
	for (int i = 1; i < argc; i++) {
		char buffer[1024];
		len += sprintf(buffer, "$%ld\r\n%s\r\n", strlen(argv[i]), argv[i]);
		strcat(message, buffer);
	}

	send(sock_fd, message, len, 0);

	char buffer[1024];
	if (recv(sock_fd, buffer, 1024, 0) == -1) {
		perror("Failed to receive data");
		return 1;
	}

	switch (buffer[0]) {
	case '+':
	case '-':
	case ':':
		printf("%s\n", buffer);
		break;
	case '$':
		strtok(buffer, "\r\n");
		char *data;
		while ((data = strtok(NULL, "\r\n")) != NULL)
			printf("%s\n", data);
		break;
	case '*':
		printf("Array response\n");
		break;
	default:
		printf("Unknown response\n");
	}

	close(sock_fd);
	return 0;
}
