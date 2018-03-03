
SRCS +=\
	   run_array.c\
	   skiplist.c\
	   bloomfilter.c\
	   lsmtree.c\
	   compaction.c\
	   page.c\
	   pageQ.c\
cache.c\

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


all:set_check $(TARGETOBJ)

DEBUG: $(TARGETDOBJ)

LEAK: $(TARGETMOBJ)

liblsm.a : $(TARGETOBJ)
	$(AR) r $(@) $(TARGETOBJ)

set_check:../../include/lsm_settings.h
	make test_clean

.c.o :
	$(CC) $(CFLAGS_ALGO) -c $< -o $@

../../object/%_d.o: %.c
	$(CC) $(CFLAGS_ALGO) -DDEBUG -c $< -o $@

../../object/%_m.o: %.c
	$(CC) $(CFLAGS_ALGO) -DLEAKCHECK -c $< -o $@

../../object/%.o: %.c
	$(CC) $(CFLAGS_ALGO) -c $< -o $@

run_test:run_array.c
	$(CC) $(CFLAGS_ALGO) -o $@ $^

test_clean :
	@$(RM) ../../object/*

test : liblsm.a
	$(CC) $(CFLAGS_ALGO) -o ttt liblsm.a test.c -lpthread

clean : 
	@$(RM) liblsm*
	@$(RM) *.o
