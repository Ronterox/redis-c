#include <stdio.h>

int main(int argc, char const *argv[]) {
	for (int i = 1; i < argc; i++) {
		printf("arg %d: %s\n", i, argv[i]);
	}
}
