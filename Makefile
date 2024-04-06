.DELETE_ON_ERROR:

server.out: clean
	gcc app/*.c -o server.out

run-server: server.out
	./server.out

cli.out: clean
	gcc bin/*.c -o cli.out

test: cli.out
	./cli.out xadd test 0-* foo bar
	./cli.out xadd test 0-* foo bar
	./cli.out xadd test 0-* foo bar
	./cli.out xadd test 0-* foo bar
	./cli.out xrange test 0-2 0-4

.PHONY: clean
clean:
	rm -f *.out
