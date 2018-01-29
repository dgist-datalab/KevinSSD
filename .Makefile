CC=gcc


LISTOFALGO:=\
			normal\
			lsmtree\

TARGET_LOWER=posix
TARGET_ALGO=normal

#LOWER_LIB=libpos.a
#ALGO_LIB=libnom.a
ALGO_LIB_D=\
		   $(patsubst %.a,%_d.a,$(ALGO_LIB))

PWD=$(pwd)

CFLAGS +=\
		 -g\
		 -std=c99\
		 -lpthread\
		 -D$(TARGET_LOWER)\
		 -D$(TARGET_ALGO)\
		 -Wall\
		 -Wno-discarded-qualifiers\

SRCS +=\
	./interface/queue.c\
	./interface/interface.c\

TARGETOBJ =\
			$(patsubst %.c,%.o,$(SRCS))\

TARGETDOBJ =\
			$(patsubst %.c,%_d.o,$(SRCS))\

MEMORYOBJ =\
		   	$(patsubst %.c,%_mem.o,$(SRCS))\

LIBS +=\
		-lpthread\

all: simulator

DEBUG : simulator_d

memory_leak: simulator_memory_check
	
simulator_memory_check: ./interface/main.c mem_libsimulator.a
	$(CC) $(CFLAGS) -DLEAKCHECK -o $@ $^ $(LIBS)

simulator: ./interface/main.c libsimulator.a $(LOWER_LIB) $(ALGO_LIB)
	$(CC) $(CFLAGS) -o $@ $^ -lpthread

simulator_d: ./interface/main.c libsimulator_d.a $(LOWER_LIB) $(ALGO_LIB_D)
	$(CC) $(CFLAGS) -DDEBUG -o $@ $^ -lpthread

libsimulator_d.a: $(TARGETDOBJ)
	make clean
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make DEBUG && cp *.a ../../ && cp *.o ../../object/ &&cd ../../
	cd ./lower/$(TARGET_LOWER) && make && cp *.a ../../ && cp *.o ../../object/ && cd ../../
	$(AR) r $(@) ./interface/*.o
	mv ./interface/*.o ./object/


libsimulator.a: $(TARGETOBJ)
	make clean
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make && cp *.a ../../ && cp *.o ../../object/ &&cd ../../
	cd ./lower/$(TARGET_LOWER) && make && cp *.a ../../ && cp *.o ../../object/ && cd ../../
	$(AR) r $(@) ./interface/*.o
	mv ./interface/*.o ./object/

mem_libsimulator.a:$(MEMORYOBJ)
	make clean
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make && cp *.a ../../ && cp *.o ../../object/ &&cd ../../
	cd ./lower/$(TARGET_LOWER) && make && cp *.a ../../ && cp *.o ../../object/ && cd ../../
	$(AR) r $(@) ./interface/*.o
	mv ./interface/*.o ./object/

%_mem.o: %.c
	$(CC) $(CFLAGS) -DLEAKCHECK -c $< -o $@ $(LIBS)

%_d.o: %.c
	$(CC) $(CFLAGS) -DDEBUG -c $< -o $@ $(LIBS)

%.o :%.c
	$(CC) $(CFLAGS) -c $< -o $@ $(LIBS)


clean :
	cd ./algorithm/$(TARGET_ALGO) && make clean && cd ../../
	cd ./lower/$(TARGET_LOWER) && make clean && cd ../../
	@$(RM) ./data/*
	@$(RM) ./object/*.o
	@$(RM) *.a
	@$(RM) simulator*
