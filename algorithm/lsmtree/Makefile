CC=gcc

CFLAGS +=\
		 -g\
		 -std=c99\
		 -Wno-discarded-qualifiers\
		 -lpthread\


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
	$(patsubst %.c,%.o,$(SRCS))\
	#$(addprefix ../../object/,$(OBJS))\

TARGETDOBJ:=\
	$(patsubst %.c,%_d.o,$(SRCS))\
	#$(addprefix ../../object/,$(OBJS))\



all: liblsm.a
	#$(TARGETOBJ) 

DEBUG: $(TARGETDOBJ)
	$(AR) r liblsm_d.a $(TARGETDOBJ)

liblsm.a : $(TARGETOBJ)
	$(AR) r $(@) $(TARGETOBJ)

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@

%_d.o: %.c
	$(CC) $(CFLAGS) -DDEBUG -c $< -o $@

../../object/%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

test_clean :
	@$(RM) ../../object/*

test : liblsm.a
	$(CC) $(CFLAGS) -o ttt liblsm.a test.c -lpthread

clean : 
	@$(RM) liblsm*
	@$(RM) *.o
