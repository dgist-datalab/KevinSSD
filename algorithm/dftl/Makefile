CC = g++

CFLAGS_ALGO +=\

SRCS +=\
	   dftl.c\
	   lru_list.c\
	   garbage_collection.c\
	   dftl_heap.c\
	   dftl_queue.c\

OBJS :=\
	$(patsubst %.c,%.o,$(SRCS))\

TARGETOBJ:=\
	$(addprefix ../../object/,$(OBJS))\

LEAK: all
all: $(TARGETOBJ) 

libnom.a : $(TARGETOBJ)
	$(AR) r $(@) $(TARGETOBJ)

.c.o :
	$(CC) $(CFLAGS_ALGO) -c $< -o $@

../../object/%.o: %.c
	$(CC) $(CFLAGS_ALGO) -c $< -o $@

clean : 
	@$(RM) libnom.a
	@$(RM) *.o
