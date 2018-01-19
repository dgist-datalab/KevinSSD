CC=gcc

CFLAGS +=\
		 -g\
		 -std=c99\
		 -Wno-discarded-qualifiers\


SRCS +=\
	   run_array.c\
	   skiplist.c\
	   bloomfilter.c\
	   c_queue.c\
	   lsmtree.c\
	   compaction.c\

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

test_clean :
	@$(RM) ../../object/*

clean : 
	@$(RM) liblsm.a
	@$(RM) *.o
