export CC=g++

TARGET_LOWER=posix_async
TARGET_ALGO=lsmtree
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


#CFLAGS_ALGO+=-DCOMPACTIONLOG\
	
CFLAGS_ALGO+=$(COMMONFLAGS)\

CFLAGS_LOWER+=$(COMMONFLAGS)\

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
		 -D_BSD_SOURCE\
-DBENCH\
-DCDF\

SRCS +=\
	./interface/queue.c\
	./interface/interface.c\
	./interface/bb_checker.c\
	./include/FS.c\
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

memory_leak: simulator_memory_check

duma_sim: duma_simulator
	
simulator_memory_check: ./interface/main.c mem_libsimulator.a
	$(CC) $(CFLAGS) -DLEAKCHECK -o $@ $^ $(ARCH) $(LIBS)

debug_simulator: ./interface/main.c libsimulator_d.a
	$(CC) $(CFLAGS) -DDEBUG -o $@ $^ $(LIBS)

simulator: ./interface/main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^ $(ARCH) $(LIBS)

duma_simulator: ./interface/main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^ -lduma $(ARCH) $(LIBS)
	

libsimulator.a: $(TARGETOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && $(MAKE) clean && $(MAKE) && cd ../../
	cd ./lower/$(TARGET_LOWER) && $(MAKE) && cd ../../ 
	mv ./interface/*.o ./object/ && mv ./bench/*.o ./object/ && mv ./include/*.o ./object/
	$(AR) r $(@) ./object/*

libsimulator_d.a:$(MEMORYOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && $(MAKE) DEBUG && cd ../../
	cd ./lower/$(TARGET_LOWER) && $(MAKE) DEBUG && cd ../../ 
	mv ./interface/*.o ./object/ && mv ./bench/*.o ./object/ && mv ./include/*.o ./object/
	$(AR) r $(@) ./object/*

mem_libsimulator.a:$(MEMORYOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && $(MAKE) LEAK && cd ../../
	cd ./lower/$(TARGET_LOWER) && $(MAKE) && cd ../../ 
	mv ./interface/*.o ./object/ & mv ./bench/*.o ./object/ && mv ./include/*.o ./object/
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
