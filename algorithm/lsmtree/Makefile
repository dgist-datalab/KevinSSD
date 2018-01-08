CC=gcc

CFLAGS +=\
		 -g\
		 -std=c99\


SRCS +=\
	   run_array.c\
	   skiplist.c\

OBJS :=\
	$(patsubst %.c,%.o,$(SRCS))\

TARGETOBJ:=\
	$(addprefix ../../object/,$(OBJS))\


all: $(TARGETOBJ) 

liblsm.a : $(TARGETOBJ)
	$(AR) r $(@) $(TARGETOBJ)

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@

../../object/%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean : 
	@$(RM) libnom.a
	@$(RM) *.o
