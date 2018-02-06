CC=gcc

TARGET_LOWER=posix
TARGET_ALGO=page
PWD=$(pwd)

CFLAGS +=\
		 -g\
		 -std=c99\
		 -lpthread\
		 -D$(TARGET_LOWER)\
		 -D$(TARGET_ALGO)\
		 -Wall\
		 -Wno-discarded-qualifiers\
		 -D_BSD_SOURCE\
-DBENCH\

SRCS +=\
	./interface/lfqueue.c\
	./interface/interface.c\
	./bench/measurement.c\
	./bench/bench.c\

TARGETOBJ =\
			$(patsubst %.c,%.o,$(SRCS))\

MEMORYOBJ =\
		   	$(patsubst %.c,%_mem.o,$(SRCS))\

DEBUGOBJ =\
		   	$(patsubst %.c,%_d.o,$(SRCS))\

LIBS +=\
		-lpthread\

all: simulator

DEBUG: debug_simulator

memory_leak: simulator_memory_check

duma_sim: duma_simulator
	
simulator_memory_check: ./interface/main.c mem_libsimulator.a $(LOWER_LIB) $(ALGO_LIB)
	$(CC) $(CFLAGS) -DLEAKCHECK -o $@ $^ $(LIBS)

debug_simulator: ./interface/main.c libsimulator_d.a
	$(CC) $(CFLAGS) -DDEBUG -o $@ $^ -lpthread

simulator: ./interface/main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^ -lpthread

duma_simulator: ./interface/main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^ -lpthread -lduma
	

libsimulator.a: $(TARGETOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make && cd ../../
	cd ./lower/$(TARGET_LOWER) && make && cd ../../ 
	mv ./interface/*.o ./object/ && mv ./bench/*.o ./object/
	$(AR) r $(@) ./object/*.o

libsimulator_d.a:$(MEMORYOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make DEBUG && cd ../../
	cd ./lower/$(TARGET_LOWER) && make DEBUG && cd ../../ 
	mv ./interface/*.o ./object/ && mv ./bench/*.o ./object/
	$(AR) r $(@) ./object/*.o

mem_libsimulator.a:$(MEMORYOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make && cd ../../
	cd ./lower/$(TARGET_LOWER) && make && cd ../../ 
	mv ./interface/*.o ./object/ & mv ./bench/*.o ./object/
	$(AR) r $(@) ./object/*.o

%_mem.o: %.c
	$(CC) $(CFLAGS) -DLEAKCHECK -c $< -o $@ $(LIBS)

%_d.o: %.c
	$(CC) $(CFLAGS) -DDEBUG -c $< -o $@ $(LIBS)

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@ $(LIBS)


clean :
	cd ./algorithm/$(TARGET_ALGO) && make clean && cd ../../
	cd ./lower/$(TARGET_LOWER) && make clean && cd ../../
	@$(RM) ./data/*
	@$(RM) ./object/*.o
	@$(RM) *.a
	@$(RM) simulator
	@$(RM) simulator_memory_check
	@$(RM) debug_simulator
	@$(RM) duma_simulator
