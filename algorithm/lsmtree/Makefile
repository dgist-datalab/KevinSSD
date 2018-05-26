ifeq ($(CC),cc)
 CC=gcc
 CFLAGS_ALGO=\
		 -g\
		 -Wall\

 ifeq ($(CC),gcc)
  CFLAGS_ALGO+=\
			   -Wno-discarded-qualifiers\
			   -std=c99\

 else
  CFLAGS_ALGO+=\
			   -std=c++11\

 endif
endif

SRCS +=\
	   run_array.c\
	   skiplist.c\
	   bloomfilter.c\
	   lsmtree.c\
	   compaction.c\
	   page.c\
	   pageQ.c\
	   heap.c\
	   footer.c\
	   log_list.c\
	   factory.c\
#cache.c\

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

.c.o :  ../../include/lsm_settings.h ../../include/settings.h
	$(CC) $(CFLAGS_ALGO) -c $< -o $@

../../object/%_d.o: %.c ../../include/lsm_settigs.h ../../include/settings.h
	$(CC) $(CFLAGS_ALGO) -DDEBUG -c $< -o $@

../../object/%_m.o: %.c ../../include/lsm_settings.h ../../include/settings.h
	$(CC) $(CFLAGS_ALGO) -DLEAKCHECK -c $< -o $@

../../object/%.o: %.c  ../../include/lsm_settings.h ../../include/settings.h
	$(CC) $(CFLAGS) $(CFLAGS_ALGO) -c $< -o $@

run_test:run_array.c
	$(CC) $(CFLAGS_ALGO) -o $@ $^

test_clean :
	@$(RM) ../../object/*

test : liblsm.a
	$(CC) $(CFLAGS_ALGO) -o ttt liblsm.a test.c -lpthread

clean : 
	@$(RM) liblsm*
	@$(RM) *.o
