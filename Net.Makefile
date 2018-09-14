export CC=g++

TARGET_LOWER=bdbm_drv
PWD=$(pwd)

COMMONFLAGS=\
			-DSLC\

export CFLAGS_LOWER=\
			-g\
			 -lpthread\
			 -Wall\
			 -D_FILE_OFFSET_BITS=64\
-O2\

export priority="true"



CFLAGS_LOWER+=$(COMMONFLAGS)\

ifeq ($(CC), gcc)
 CFLAGS_LOWER+=-Wno-discarded-qualifiers -std=c99
else
 CFLAGS_LOWER+= -std=c++11
endif

CFLAGS +=\
		 -D$(TARGET_LOWER)\
		 -D_BSD_SOURCE\
-DBENCH\
-DCDF\
-DSLC\
-Wall\
-O2\


SRCS +=\
	./interface/queue.c\
	./interface/bb_checker.c\
	./include/FS.c\
	./include/dl_sync.c\
	./include/rwlock.c\
	./include/data_struct/hash.c\
	./include/data_struct/list.c\
	./include/utils/thpool.c\
	./bench/measurement.c\
	./bench/bench.c\

TARGETOBJ =\
			$(patsubst %.c,%.o,$(SRCS))\

ifeq ($(TARGET_LOWER),bdbm_drv)
	ARCH +=./object/libmemio.a
endif

LIBS +=\
		-lpthread\
		-lm\

all : server

server: ./interface/network/network_main.c libsimulator.a
	$(CC) $(CFLAGS_LOWER) $(CFLAGS)  -o $@ $^  $(ARCH) $(LIBS)

libsimulator.a:$(TARGETOBJ)
	mkdir -p object && mkdir -p data
	cd ./lower/$(TARGET_LOWER) && $(MAKE) && cd ../../ 
	cd ./include/kuk_socket_lib/ && $(MAKE) && mv ./*.o ../../object/ && cd ../../
	mv ./include/data_struct/*.o ./object/
	mv ./include/utils/*.o ./object/
	mv ./interface/*.o ./object/ && mv ./bench/*.o ./object/ && mv ./include/*.o ./object/
	$(AR) r $(@) ./object/*

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@ $(LIBS)

clean :
	cd ./lower/$(TARGET_LOWER) && $(MAKE) clean && cd ../../
	@$(RM) ./data/*
	@$(RM) ./object/*.o
	@$(RM) *.a
	@$(RM) server
