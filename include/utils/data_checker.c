#include "../settings.h"
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

uint32_t *keymap;
static uint32_t my_seed;

int str2int(const char* str, int len)
{
    int i;
    int ret = 0;
    for(i = 0; i < len; ++i)
    {
        ret = ret * 10 + (str[i] - '0');
    }
    return ret;
}

void __checking_data_init(){
	keymap=(uint32_t *)malloc(sizeof(uint32_t)*SHOWINGSIZE);
}

void __checking_data_make(uint32_t key, char *data){
	keymap[key]=my_seed;
	srand(my_seed);
	for(uint32_t i=0; i<1024; i++){
		uint32_t t=rand();
		memcpy(&data[i*sizeof(uint32_t)], &t, sizeof(uint32_t));
	}
	my_seed++;
}

void __checking_data_make_key(KEYT _key, char *data){
	uint32_t key=str2int(_key.key, _key.len);
	keymap[key]=my_seed;
	srand(my_seed);
	for(uint32_t i=0; i<1024; i++){
		uint32_t t=rand();
		memcpy(&data[i*sizeof(uint32_t)], &t, sizeof(uint32_t));
	}
	my_seed++;
}

bool __checking_data_check_key(KEYT _key, char *data){
	uint32_t key=str2int(_key.key, _key.len);
	uint32_t test_seed=keymap[key];
	uint32_t t;
	srand(test_seed);
	for(uint32_t i=0; i<1024; i++){
		memcpy(&t, &data[i*sizeof(uint32_t)], sizeof(uint32_t));
		if(rand() != t){
			printf("data miss!!!!\n");
			abort();
		}
	}
	return true;
}

bool __checking_data_check(uint32_t key, char *data){
	uint32_t test_seed=keymap[key];
	uint32_t t;
	srand(test_seed);
	for(uint32_t i=0; i<1024; i++){
		memcpy(&t, &data[i*sizeof(uint32_t)], sizeof(uint32_t));
		if(rand() != t){
			printf("data miss!!!!\n");
			abort();
		}
	}
	return true;
}

void __checking_data_free(){
	free(keymap);
}
