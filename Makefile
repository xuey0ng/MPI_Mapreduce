build:
	mpicc -o a03 tasks.c utils.c main.c
	# mpicc -o fullasync tasks.c utils.c main_iter2.c