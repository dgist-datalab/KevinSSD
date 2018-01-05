CC=gcc

CFLAGS +=\
		 -g\
		 -std=c99\
		 -D_FILE_OFFSET_BITS=64\

SRCS +=\
	   posix.c\

OBJS :=\
	$(patsubst %.c,%.o,$(SRCS))\

TARGETOBJ:=\
	$(addprefix ../../object/,$(OBJS))\

all: $(TARGETOBJ)

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@

../../object/%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean : 
	@$(RM) libpos.a
	@$(RM) *.o
