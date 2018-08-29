export CC=g++

TARGET_INF=interface
TARGET_LOWER=bdbm_drv
TARGET_ALGO=Lsmtree
PWD=$(pwd)

COMMONFLAGS=\
			-DSLC\

export CFLAGS_ALGO=\
			 -g\
			 -Wall\
			 -D$(TARGET_LOWER)\
#-DDVALUE\


export CFLAGS_LOWER=\
			-g\
			 -lpthread\
			 -Wall\
			 -D_FILE_OFFSET_BITS=64\

export priority="true"


#CFLAGS_ALGO+=-DCOMPACTIONLOG\
	
CFLAGS_ALGO+=$(COMMONFLAGS)\
			 -D$(TARGET_ALGO)\

CFLAGS_LOWER+=$(COMMONFLAGS)\
			  -D$(TARGET_ALGO)\

ifeq ($(TARGET_ALGO), lsmtree)
 CFLAGS_ALGO+=-DLSM_SKIP
endif

ifeq ($(CC), gcc)
 CFLAGS_ALGO+=-Wno-discarded-qualifiers -std=c99
 CFLAGS_LOWER+=-Wno-discarded-qualifiers -std=c99
else
 CFLAGS_ALGO+= -std=c++11
 CFLAGS_LOWER+= -std=c++11
endif

CFLAGS +=\
		 $(CFLAGS_ALGO)\
		 -D$(TARGET_LOWER)\
		 -D$(TARGET_ALGO)\
		 -D$(TARGET_INF)\
		 -D_BSD_SOURCE\
-DBENCH\
-DCDF\

SRCS +=\
	./interface/queue.c\
	./interface/interface.c\
	./interface/bb_checker.c\
	./include/FS.c\
	./include/dl_sync.c\
	./include/data_struct/hash.c\
	./bench/measurement.c\
	./bench/bench.c\

TARGETOBJ =\
			$(patsubst %.c,%.o,$(SRCS))\

MEMORYOBJ =\
		   	$(patsubst %.c,%_mem.o,$(SRCS))\

DEBUGOBJ =\
		   	$(patsubst %.c,%_d.o,$(SRCS))\


ifeq ($(TARGET_LOWER),bdbm_drv)
	ARCH +=./object/libmemio.a
endif

LIBS +=\
		-lpthread\
		-lm\

all: simulator

DEBUG: debug_simulator

duma_sim: duma_simulator

debug_simulator: ./interface/main.c libsimulator_d.a
	$(CC) $(CFLAGS) -DDEBUG -o $@ $^ $(LIBS)

simulator: ./interface/B_main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^  $(ARCH) $(LIBS)

duma_simulator: ./interface/main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^ -lduma $(ARCH) $(LIBS)
	

libsimulator.a: $(TARGETOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && $(MAKE) clean && $(MAKE) && cd ../../
	cd ./lower/$(TARGET_LOWER) && $(MAKE) && cd ../../ 
	cd ./algorithm/blockmanager && $(MAKE) && cd ../../
	cd ./include/kuk_socket_lib/ && $(MAKE) && mv ./*.o ../../object/ && cd ../../
	mv ./include/data_struct/*.o ./object/
	mv ./interface/*.o ./object/ && mv ./bench/*.o ./object/ && mv ./include/*.o ./object/
	$(AR) r $(@) ./object/*

%_mem.o: %.c
	$(CC) $(CFLAGS) -DLEAKCHECK -c $< -o $@ $(LIBS)

%_d.o: %.c
	$(CC) $(CFLAGS) -DDEBUG -c $< -o $@ $(LIBS)

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@ $(LIBS)


clean :
	cd ./algorithm/$(TARGET_ALGO) && $(MAKE) clean && cd ../../
	cd ./lower/$(TARGET_LOWER) && $(MAKE) clean && cd ../../
	@$(RM) ./data/*
	@$(RM) ./object/*.o
	@$(RM) *.a
	@$(RM) simulator
	@$(RM) simulator_memory_check
	@$(RM) debug_simulator
	@$(RM) duma_simulator
