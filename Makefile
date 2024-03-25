.DELETE_ON_ERROR:

server.out:
	gcc app/*.c -o server.out

run-server: server.out
	./server.out

cli.out:
	gcc bin/*.c -o cli.out

.PHONY: clean
clean:
	rm -f *.out
