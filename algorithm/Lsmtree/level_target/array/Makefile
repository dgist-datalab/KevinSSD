SRCS +=\
	   array.c\
	   array_level.c\

OBJS :=\
	$(patsubst %.c,%.o,$(SRCS))\


all:$(OBJS)
	mv *.o ../../../../object/

test: test.c	
	gcc -g -o test test.c

.c.o:
	$(CC) $(CFLAGS_ALGO) -c $< -o $@
