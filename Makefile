CC=gcc

TARGET_LOWER=posix
TARGET_ALGO=normal
PWD=$(pwd)

CFLAGS +=\
		 -g\
		 -std=c99\
		 -lpthread\
		 -D$(TARGET_LOWER)\
		 -D$(TARGET_ALGO)\
		 -Wall\
		 -Wno-discarded-qualifiers\
		 -DLEAKCHECK\

SRCS +=\
	./interface/queue.c\
	./interface/interface.c\

TARGETOBJ =\
			$(patsubst %.c,%.o,$(SRCS))

all: simulator
	
simulator: ./interface/main.c libsimulator.a
	$(CC) $(CFLAGS) -o $@ $^ -lpthread

libsimulator.a: $(TARGETOBJ)
	mkdir -p object && mkdir -p data
	cd ./algorithm/$(TARGET_ALGO) && make && cd ../../
	cd ./lower/$(TARGET_LOWER) && make && cd ../../ 
	mv ./interface/*.o ./object/
	$(AR) r $(@) ./object/*.o

.c.o :
	$(CC) $(CFLAGS) -c $< -o $@ -lpthread

clean :
	cd ./algorithm/$(TARGET_ALGO) && make clean && cd ../../
	cd ./lower/$(TARGET_LOWER) && make clean && cd ../../
	@$(RM) ./data/*
	@$(RM) ./object/*.o
	@$(RM) *.a
	@$(RM) simulator
