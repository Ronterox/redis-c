.DELETE_ON_ERROR:

server.out: clean
	gcc app/*.c -o server.out

run-server: server.out
	./server.out

cli.out: clean
	gcc bin/*.c -o cli.out

testread: cli.out
	./cli.out xadd mango 0-2 temperature 2
	./cli.out xread block 0 streams mango \$

.PHONY: clean
clean:
	rm -f *.out
