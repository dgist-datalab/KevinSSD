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
	   lsmtree.c\
	   compaction.c\

OBJS :=\
	$(patsubst %.c,%.o,$(SRCS))\

TARGETOBJ:=\
	$(addprefix ../../object/,$(OBJS))\
#$(patsubst %.c,%.o,$(SRCS))\

DOBJ:=\
	$(patsubst %.c,%_d.o,$(SRCS))\

TARGETDOBJ=\
	$(addprefix ../../object/,$(DOBJ))\


MOBJS :=\
	$(patsubst %.c,%_m.o,$(SRCS))\

TARGETMOBJ:=\
	$(addprefix ../../object/,$(MOBJS))\


all:$(TARGETOBJ)

DEBUG: $(TARGETDOBJ)

LEAK: $(TARGETMOBJ)

liblsm.a : $(TARGETOBJ)
	$(AR) r $(@) $(TARGETOBJ)

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@

../../object/%_d.o: %.c
	$(CC) $(CFLAGS) -DDEBUG -c $< -o $@

../../object/%_m.o: %.c
	$(CC) $(CFLAGS) -DLEAKCHECK -c $< -o $@

../../object/%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

run_test:run_array.c
	$(CC) $(CFLAGS) -o $@ $^

test_clean :
	@$(RM) ../../object/*

test : liblsm.a
	$(CC) $(CFLAGS) -o ttt liblsm.a test.c -lpthread

clean : 
	@$(RM) liblsm*
	@$(RM) *.o
