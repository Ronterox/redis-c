.DELETE_ON_ERROR:

server.out: clean
	gcc app/*.c -o server.out

run-server: server.out
	./server.out

cli.out: clean
	gcc bin/*.c -o cli.out

testblock: cli.out
	./cli.out xadd test 0-1 foo bar
	./cli.out xread block 1000 streams test 0-1 &
	./cli.out xadd test 0-2 foo bar

testread: cli.out
	./cli.out xadd "mango" "0-2" temperature 2
	./cli.out xread streams mango 0-1

.PHONY: clean
clean:
	rm -f *.out
