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
#define is_str_equaln(str1, str2, n) (strncmp(str1, str2, n) == 0)

#define cmd_switch(str) if is_str_equal (command, str)
#define cmd_case(str) else if is_str_equal (command, str)

#define DATABASE_START '\xFE'
#define EXPIRE_MS '\xFC'
#define EXPIRE_SEC '\xFD'
#define STRING '\x00'

typedef struct {
	char *key;
	char *value;
	time_t ttl;
} KeyValue;

typedef struct {
	time_t ms;
	int seq;
} Sequence;

typedef struct {
	char *key;
	char *id[10];
	KeyValue keyvs[10];
	Sequence id_seq;
} Stream;

typedef struct Server {
	int port;
	int fd;
	char *host;
	char *directory;
	char *dbfilename;
	struct Server *replicaof;
} Server;

KeyValue keyvs[KEYS_SIZE];
int keyvs_size = 0;

int repl_fd[10] = {0};
int repl_size = 0;

Stream streams[KEYS_SIZE];
int streams_size = 0;

Sequence sequences[KEYS_SIZE] = {{.ms = 0, .seq = 1}};
int sequences_size = 1;

int ack = 0;
Server server = {.port = 6379, .host = "localhost"};

time_t currentMillis() {
	struct timeval tp;

	gettimeofday(&tp, NULL);
	return tp.tv_sec * 1000 + tp.tv_usec / 1000;
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

int get_key_index(char *key) {
	fori(i, keyvs_size) {
		if is_str_equal (key, keyvs[i].key)
			return i;
	}
	return -1;
}

int get_stream_index(char *key) {
	int mslen = strchr(key, '-') - key;
	fori(i, streams_size) {
		if is_str_equaln (key, streams[i].key, mslen)
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
	return index == -1 ? NULL : keyvs[index].value;
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

void set_key_value(char *key, char *value, char *ttl) {
	int index = get_key_index(key);
	time_t key_ttl = ttl == NULL ? 0 : atoi(ttl) + currentMillis();
	if (index == -1) {
		keyvs[keyvs_size].key = strdup(key);
		keyvs[keyvs_size].value = strdup(value);
		keyvs[keyvs_size].ttl = key_ttl;
		keyvs_size++;
	} else {
		free(keyvs[index].value);
		keyvs[index].value = strdup(value);
		keyvs[index].ttl = key_ttl;
	}
}

int next_sequence(time_t ms) {
	fori(i, sequences_size) {
		if (sequences[i].ms == ms) {
			return sequences[i].seq++;
		}
	}
	sequences[sequences_size].ms = ms;
	sequences[sequences_size].seq = 1;
	sequences_size++;
	return 0;
}

void parse_id(char *id, time_t *ms, int *seq) {
	int index = strchr(id, '-') - id;
	id[index] = '\0';

	*ms = strtoul(id, NULL, 10);
	if (id[index + 1] == '*') {
		*seq = next_sequence(*ms);
		sprintf(id, "%ld-%d", *ms, *seq);
	} else {
		*seq = atoi(id + index + 1);
	}
	id[index] = '-';
}

void set_stream(int client_fd, char *key, char *id, char **data, int dsize) {
	if (id[0] == '*' && id[1] == '\0') {
		time_t ms = currentMillis();
		sprintf(id, "%ld-%d", ms, next_sequence(ms));
	}

	time_t ms;
	int seq;
	parse_id(id, &ms, &seq);

	if (seq <= 0 && ms <= 0) {
		send(client_fd,
			 "-ERR The ID specified in XADD must be greater than 0-0\r\n", 56,
			 0);
		return;
	}

	time_t ms_i;
	int seq_i;
	Stream *stream;
	fori(i, streams_size) {
		stream = &streams[i];
		parse_id(stream->id[stream->id_seq.seq - 1], &ms_i, &seq_i);

		if (ms < ms_i || ms == ms_i && seq <= seq_i) {
			send(client_fd,
				 "-ERR The ID specified in XADD is equal or smaller than "
				 "the target stream top item\r\n",
				 83, 0);
			return;
		}
	}

	int index = get_stream_index(key);
	if (index == -1)
		index = streams_size++;

	stream = &streams[index];
	stream->key = strdup(key);

	index = stream->id_seq.seq;
	stream->id[index] = strdup(id);
	stream->keyvs[index].key = strdup(data[0]);
	stream->keyvs[index].value = strdup(data[1]);
	stream->id_seq.seq++;
	printf("Stream: %s\n", stream->key);

	char buffer[BUFFER_SIZE] = {0};
	int len = sprintf(buffer, "$%lu\r\n%s\r\n", strlen(id), id);
	send(client_fd, buffer, len, 0);
}

void set(int client_fd, char *key, char *value, char *ttl) {
	if (key == NULL || value == NULL) {
		send(client_fd, "-Missing arguments\r\n", 6, 0);
		return;
	}

	set_key_value(key, value, ttl);

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
	time_t ttl = keyvs[index].ttl;
	if (index == -1 || ttl > 0 && currentMillis() > ttl) {
		send(client_fd, "$-1\r\n", 5, 0);
	} else {
		char buffer[BUFFER_SIZE] = {0};
		char *value = keyvs[index].value;
		int len = sprintf(buffer, "$%lu\r\n%s\r\n", strlen(value), value);
		send(client_fd, buffer, len, 0);
	}
}

void type(int client_fd, char *key) {
	if (key == NULL) {
		send(client_fd, "-Missing key argument\r\n", 6, 0);
		return;
	}

	int index = get_key_index(key);
	int index_stream = get_stream_index(key);
	time_t ttl = keyvs[index].ttl;
	if (index == -1 && index_stream == -1 || ttl > 0 && currentMillis() > ttl) {
		send(client_fd, "+none\r\n", 7, 0);
	} else {
		send(client_fd, index == -1 ? "+stream\r\n" : "+string\r\n", 9, 0);
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
	if (key == NULL) {
		send(client_fd, "-Missing argument\r\n", 6, 0);
		return;
	}

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

void config(int client_fd, char *key, char *value) {
	if is_str_equal (key, "get") {
		char buffer[BUFFER_SIZE] = {0};
		char *data =
			is_str_equal(value, "dir") ? server.directory : server.dbfilename;

		int len = sprintf(buffer, "*2\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n",
						  strlen(value), value, strlen(data), data);
		send(client_fd, buffer, len, 0);
	}
}

void keys(int client_fd, char *key) {
	if (key == NULL) {
		send(client_fd, "-Missing argument\r\n", 6, 0);
		return;
	}

	if (key[0] == '*' && key[1] == '\0') {
		char buffer[BUFFER_SIZE] = {0};
		int len = sprintf(buffer, "*%d\r\n", keyvs_size);
		fori(i, keyvs_size) {
			len += sprintf(buffer + len, "$%lu\r\n%s\r\n", strlen(keyvs[i].key),
						   keyvs[i].key);
		}
		send(client_fd, buffer, len, 0);
	}
}

void xrange(int client_fd, char *key, char *start, char *end) {
	int index = get_stream_index(key);
	if (index == -1) {
		send(client_fd, "$-1\r\n", 5, 0);
		return;
	}

	time_t ms;
	int seq_start, seq_end;
	if (*start == '-') {
		seq_start = 0;
	} else {
		parse_id(start, &ms, &seq_start);
	}

	Stream *stream = &streams[index];
	KeyValue *kv;

	int stm_elem_size = stream->id_seq.seq;
	if (*end == '+') {
		seq_end = stm_elem_size;
	} else {
		parse_id(end, &ms, &seq_end);
	}

	seq_start = seq_start == 0 ? 0 : seq_start - 1;
	int diff = seq_end - seq_start;

	char buffer[BUFFER_SIZE] = {0};
	int len =
		sprintf(buffer, "*%d\r\n", diff > stm_elem_size ? stm_elem_size : diff);
	for (int i = seq_start; i < seq_end && i < stm_elem_size; i++) {
		len += sprintf(buffer + len, "*2\r\n$%lu\r\n%s\r\n*2\r\n",
					   strlen(stream->id[i]), stream->id[i]);
		kv = &stream->keyvs[i];
		len += sprintf(buffer + len, "$%lu\r\n%s\r\n$%lu\r\n%s\r\n",
					   strlen(kv->key), kv->key, strlen(kv->value), kv->value);
	}
	send(client_fd, buffer, len, 0);
}

int xread(char *buffer, char *key, char *start) {
	int index = get_stream_index(key);
	if (index == -1)
		return 0;

	time_t ms;
	int seq_start, seq_end;
	parse_id(start, &ms, &seq_start);

	Stream *stream = &streams[index];
	seq_end = stream->id_seq.seq;

	seq_start = seq_start == 0 ? 0 : seq_start - 1;
	int diff = seq_end - seq_start;

	KeyValue *kv;
	int len = sprintf(buffer, "*2\r\n$%lu\r\n%s\r\n*%d\r\n", strlen(key), key,
					  diff > seq_end ? seq_end : diff);
	for (int i = seq_start; i < seq_end && i < seq_end; i++) {
		len += sprintf(buffer + len, "*2\r\n$%lu\r\n%s\r\n*2\r\n",
					   strlen(stream->id[i]), stream->id[i]);

		kv = &stream->keyvs[i];
		len += sprintf(buffer + len, "$%lu\r\n%s\r\n$%lu\r\n%s\r\n",
					   strlen(kv->key), kv->key, strlen(kv->value), kv->value);
	}
	return len;
}

// Returns -> 0: success, 1: resend to replicas
int evaluate_commands(char **commands, int num_args, int client_fd) {
	char *command = to_lowercase(commands[0]);
	char *key = commands[1];
	char *value = commands[2];

	cmd_switch("ping") {
		ping(client_fd);
		ack += 14;
	}
	cmd_case("echo") { echo(client_fd, key); }
	cmd_case("set") {
		char *ttl = 4 < num_args ? commands[4] : NULL;
		set(client_fd, key, value, ttl);
		// *n\r\n$3\r\nset\r\n$n\r\nkey\r\n$n\r\nvalue\r\n
		ack += 25 + strlen(key) + strlen(value);
		// $2\r\npx\r\n$n\r\nttl\r\n
		if (ttl != NULL)
			ack += 14 + strlen(ttl);
		return 1;
	}
	cmd_case("get") { get(client_fd, key); }
	cmd_case("info") {
		key = to_lowercase(key);
		info(client_fd, key);
	}
	cmd_case("replconf") {
		key = to_lowercase(key);
		replconf(client_fd, key);
		ack += 37;
	}
	cmd_case("psync") {
		psync(client_fd);
		repl_fd[repl_size++] = client_fd;
	}
	cmd_case("wait") {
		int len = sprintf(key, ":%d\r\n", repl_size);
		send(client_fd, key, len, 0);
	}
	cmd_case("config") {
		key = to_lowercase(key);
		config(client_fd, key, value);
	}
	cmd_case("keys") { keys(client_fd, key); }
	cmd_case("type") { type(client_fd, key); }
	cmd_case("xadd") {
		set_stream(client_fd, key, value, commands + 3, num_args);
	}
	cmd_case("xrange") {
		char *start = commands[2];
		char *end = commands[3];
		xrange(client_fd, key, start, end);
	}
	cmd_case("xread") {
		if is_str_equal (key, "block") {
			int wait = atoi(value);
			usleep(wait);
			commands += 4;
		} else {
			commands += 2;
		}

		char buffer[BUFFER_SIZE] = {0};

		int key_size = (num_args - 2) * 0.5;
		int len = sprintf(buffer, "*%d\r\n", key_size);
		fori(i, key_size) {
			len += xread(buffer + len, commands[i], commands[i + key_size]);
		}
		send(client_fd, buffer, len, 0);
	}

	return 0;
}

void *handle_client(void *args) {
	int client_fd = *(int *)args;
	printf("Client connected %d\n", client_fd);
	while (1) {
		char buffer[BUFFER_SIZE] = {0};
		if (recv(client_fd, buffer, BUFFER_SIZE, 0) == -1) {
			perror("Failed to receive data from client");
			break;
		}

		if (buffer[0] == '\0')
			continue;

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
					fori(i, repl_size) {
						if (repl_fd[i] != client_fd) {
							printf("Sending to replica %d\n", repl_fd[i]);
							send(repl_fd[i], buffer, strlen(buffer), 0);
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

	printf("Replication handshake successful\n");
	handle_client(&server.replicaof->fd);

	return NULL;
}

void read_rdb() {
	char filepath[BUFFER_SIZE] = {0};
	sprintf(filepath, "%s/%s", server.directory, server.dbfilename);

	FILE *file = fopen(filepath, "rb");
	if (file != NULL) {
		char data[BUFFER_SIZE] = {0};

		fread(data, sizeof(char), 5, file);
		if (!is_str_equal(data, "REDIS")) {
			printf("Invalid RDB file\n");
			return;
		}
		printf("Magic: %s\n", data);

		fread(data, sizeof(char), 4, file);
		printf("RDB Version: %s\n", data);

		char byte;
		memset(data, 0, 5);
		while (fread(&byte, sizeof(char), 1, file)) {
			if (byte != DATABASE_START)
				continue;

			fseek(file, 2, SEEK_CUR);

			fread(data, sizeof(char), 2, file);
			int keys = (int)data[0];
			int expires = (int)data[1];

			printf("Number of keys: %d\n", keys);
			printf("Expires: %d\n", expires);

			int len;
			char *ttl;
			fori(i, keys) {
				fread(&byte, sizeof(char), 1, file);

				short is_ms = byte == EXPIRE_MS;
				if (is_ms || byte == EXPIRE_SEC) {
					len = is_ms ? 8 : 4;
					fread(data, sizeof(char), len, file);
					ttl = strdup(data);

					fread(&byte, sizeof(char), 1, file);
				} else {
					ttl = NULL;
				}

				if (byte == STRING) {
					fread(&len, sizeof(char), 1, file);

					char key[len];
					key[len] = '\0';
					fread(key, sizeof(char), len, file);
					fread(&len, sizeof(char), 1, file);

					char value[len];
					value[len] = '\0';
					fread(value, sizeof(char), len, file);

					set_key_value(key, value, NULL);
					if (ttl != NULL) {
						keyvs[keyvs_size - 1].ttl = (time_t)ttl;
					}
					printf("Loaded key: %s\nValue: %s\nTTL: %ld\n", key, value,
						   (time_t)ttl);
				} else {
					printf("Ignoring value of type: %d\n", data[0]);
				}
			}
		}
		fclose(file);

		printf("Loaded %d keys from %s\n", keyvs_size, filepath);
	} else {
		printf("Starting with empty database\n");
	}
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
		{"dir", required_argument, NULL, 'd'},
		{"dbfilename", required_argument, NULL, 'f'},
	};

	int opt;
	while ((opt = getopt_long(argc, (char *const *)argv, "p:r:d:f",
							  long_options, NULL)) != -1) {
		if (opt == 'p') {
			server.port = atoi(optarg);
		} else if (opt == 'r') {
			server.replicaof = malloc(sizeof(Server));
			server.replicaof->host = optarg;
			server.replicaof->port = atoi(argv[optind]);
		} else if (opt == 'd') {
			server.directory = optarg;
		} else if (opt == 'f') {
			server.dbfilename = optarg;
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

	if (server.directory != NULL && server.dbfilename != NULL) {
		read_rdb();
	}

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

	int connection_backlog = 128;
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

		int result = send_to_thread(handle_client, &client_fd);
		if (result != 0) {
			perror("Error during client to thread\n");
			return 1;
		}
	}

	if (server.replicaof != NULL) {
		close(server.replicaof->fd);
		free(server.replicaof);
	}

	fori(i, keyvs_size) {
		free(keyvs[i].key);
		free(keyvs[i].value);
	}

	close(server_fd);

	return 0;
}
