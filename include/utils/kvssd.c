#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include "../settings.h"
char* kvssd_tostring(KEYT key){
	/*
	char temp1[255]={0,};
	memcpy(temp1,key.key,key.len);*/
	return key.key;
}

void *kvssd_cpy_key(KEYT *des, KEYT *key){
	des->key=(char*)malloc(sizeof(char)*key->len);
	des->len=key->len;
	memcpy(des->key,key->key,key->len);
}
