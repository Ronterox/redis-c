.DELETE_ON_ERROR:

server.out: clean
	gcc app/*.c -o server.out

run-server: server.out
	./server.out

cli.out: clean
	gcc bin/*.c -o cli.out

test: cli.out
	./cli.out xadd test 0-1 foo bar
	./cli.out xread streams test 0-0

.PHONY: clean
clean:
	rm -f *.out
