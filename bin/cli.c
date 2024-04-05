#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#define SPACES "          "

int spaces = -1;

char *parse_data(char *buffer) {
	int index, len;
	switch (*buffer) {
	case '+':
	case '-':
	case ':':
		printf("%s\n", buffer);
		break;
	case '$':
		len = 0;
		buffer++;
		while (*buffer != '\r') {
			len = len * 10 + (*buffer - '0');
			buffer++;
		}
		printf("%.*s\"%.*s\"\n", spaces, SPACES, len, buffer + 2);
		buffer += len + 4;
		break;
	case '*':
		len = buffer[1] - '0';
		buffer += 4;
		spaces++;
		printf("%.*s[\n", spaces, SPACES);
		for (int i = 0; i < len; i++) {
			buffer = parse_data(buffer);
		}
		printf("%.*s]\n", spaces, SPACES);
		spaces--;
		break;
	default:
		printf("Unknown response\n");
	}
	return buffer;
}

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

	char buffer[1024] = {0};
	if (recv(sock_fd, buffer, 1024, 0) == -1) {
		perror("Failed to receive data");
		return 1;
	}

	// printf("Buffer %s", buffer);
	parse_data(buffer);

	close(sock_fd);
	return 0;
}
