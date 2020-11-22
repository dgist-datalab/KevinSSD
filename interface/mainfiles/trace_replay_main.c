#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <signal.h>
#include "../../include/FS.h"
#include "../../include/settings.h"
#include "../../include/types.h"
#include "../../bench/bench.h"
#include "../interface.h"
#include "../vectored_interface.h"
#include "../koo_hg_inf.h"
#include "../../include/utils/kvssd.h"
#include <map>
#include <string>
using namespace std;
static map<string, uint32_t> chk_data;

bool trace_end_req(request *const res);

static string convertToString(char *a ,int size){
	int i; 
    string s = ""; 
    for (i = 0; i < size; i++) { 
        s = s + a[i]; 
    } 
    return s; 
}

static inline void map_crc_insert(KEYT key, uint32_t crc){
	string a=convertToString(key.key, key.len);
	map<string, uint32_t>::iterator it=chk_data.find(a);
	if(it==chk_data.end()){
		chk_data.insert(pair<string, uint32_t>(a, crc));
	}
	else{
		it->second=crc;
	}
	if(key_const_compare(key, 'd', 227, 29803, NULL)){
		printf("target -> crc:%u\n", crc);
	}
}

static inline void map_crc_check(KEYT key, uint32_t input){
	string a=convertToString(key.key, key.len);
	map<string, uint32_t>::iterator it=chk_data.find(a);
	if(it==chk_data.end()){
		printf("no key!!!!!\n");
	//	print_key(key);
		return;
	}

	if(input!=it->second){
		char buf[100];
		key_interpreter(key, buf);
		printf("key:%s differ crc32 original:%u input:%u\n", buf, it->second, input);
		if(it->second==0){
			printf("value was deleted!!!\n");
		}
		abort();
	}
}

static inline void map_crc_range_delete(request *const req){
	KEYT key;
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->temp_key);
	for(uint32_t i=0; i<req->offset; i++){
		if(i==0){
			key=req->key;
		}else{
			kvssd_cpy_key(&key, &copied_key);
			uint64_t temp=*(uint64_t*)&key.key[key.len-sizeof(uint64_t)];
			temp=Swap8Bytes(temp);
			temp+=i;
			*(uint64_t*)&key.key[key.len-sizeof(uint64_t)]=Swap8Bytes(temp);
		}
		map_crc_insert(key, 0);
	}
}

void log_print(int sig){
	free_koo();
	inf_free();
	fflush(stdout);
	fflush(stderr);
	sync();
	exit(1);
}

static inline FSTYPE koo_to_req(uint8_t t){
	switch(t){
		case LIGHTFS_META_GET:
		case LIGHTFS_DATA_GET:
			return FS_GET_T;
		case LIGHTFS_DATA_SEQ_SET:
		case LIGHTFS_META_SET:
		case LIGHTFS_META_SYNC_SET:
		case LIGHTFS_DATA_SET:
			return FS_SET_T;
		case LIGHTFS_META_DEL:
		case LIGHTFS_DATA_DEL:
			return FS_DELETE_T;
		case LIGHTFS_DATA_DEL_MULTI:
			return FS_RANGEDEL_T;
		case LIGHTFS_META_CURSOR:
		case LIGHTFS_DATA_CURSOR:
			return FS_RANGEGET_T;
		case LIGHTFS_META_UPDATE:
		case LIGHTFS_DATA_UPDATE:
			return FS_RMW_T;
		case LIGHTFS_COMMIT:
			return FS_TRANS_COMMIT;
		case LIGHTFS_GET_MULTI:
			return FS_MGET_T;
		default:
			printf("no match type %s:%d\n", __FILE__, __LINE__);
			abort();
			break;
	}
	return FS_SET_T;
}
static uint32_t my_seq=0;
static inline void make_mget_req(vec_request *res, request *req, uint32_t size){
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->key);

	free(res->req_array);
	my_seq--;
	res->size=size;
	res->req_array=(request*)calloc(size, sizeof(request));
	for(uint32_t i=0; i<size; i++){
		request *temp=&res->req_array[i];
		temp->tid=res->tid;
		temp->parents=res;
		temp->end_req=trace_end_req;
		temp->isAsync=ASYNC;
		temp->seq=my_seq++;
		temp->type=FS_MGET_T;
		kvssd_cpy_key(&temp->key, &copied_key);

		uint64_t temp_k=*(uint64_t*)&temp->key.key[temp->key.len-sizeof(uint64_t)];
		temp_k=Swap8Bytes(temp_k);
		temp_k+=i;
		*(uint64_t*)&temp->key.key[temp->key.len-sizeof(uint64_t)]=Swap8Bytes(temp_k);
		temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
	}

	kvssd_free_key_content(&copied_key);
}

int main(int argc,char* argv[]){
	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);
	printf("signal add!\n");
	inf_init(1,0,argc,argv);
	FILE *f=fopen("/home/koo/trace_data2", "r");
	if(!f){
		printf("file open error!\n");
		return 0;
	}

	uint32_t tid;
	uint32_t type;
	uint32_t key_len;
	char key_type[2];
	uint64_t parents_inode, block_num;
	int req_num=0;
	while(fscanf(f, "%d%d",&tid, &type)!=EOF){
	//	printf("req:%u\n", req_num);
		vec_request *req=(vec_request*)calloc(1, sizeof(vec_request));
		req->tid=tid;
		req->size=1;
		req->req_array=(request*)calloc(req->size, sizeof(request));
		req->end_req=NULL;

		request *treq=&req->req_array[0];
		treq->type=type;
		treq->tid=tid;
		treq->isAsync=ASYNC;
		treq->end_req=trace_end_req;
		treq->parents=req;
		treq->seq=my_seq++;
		if(type==FS_TRANS_COMMIT){
			treq->key.len=0;
			assign_vectored_req(req);
			if((++req_num)%10000==0){
				printf("%u\n", req_num);
			}
			continue;
		}
		/*make key*/
		KEYT temp_key;
		fscanf(f,"%d%s",&key_len, key_type);
		temp_key.len=key_len;
		temp_key.key=(char*)malloc(key_len);
		temp_key.key[0]=key_type[0];
		fscanf(f,"%lu",&parents_inode);
		*((uint64_t*)&temp_key.key[1])=Swap8Bytes(parents_inode);
		if(key_type[0]=='m'){
			if(key_len!=10){
				fscanf(f,"%s",&temp_key.key[9]);
			}
		}
		else{
			fscanf(f,"%lu",&block_num);
			*((uint64_t*)&temp_key.key[9])=Swap8Bytes(block_num);	
		}

	//	char buf[100];
	//	key_interpreter(temp_key, buf);
	//	printf("key:%s\n", buf);

		treq->key=temp_key;
		uint32_t offset,length;
		if(type==FS_MGET_T){
			fscanf(f, "%u", &length);
			make_mget_req(req, treq, length);
			assign_vectored_req(req);
			if((++req_num)%10000==0){
				printf("%u\n", req_num);
			}
			continue;
		}
		

		fscanf(f, "%u%u", &offset, &length);
		switch(treq->type){
			case FS_GET_T:
				treq->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				treq->length=length;
				break;
			case FS_SET_T:
				kvssd_cpy_key(&treq->temp_key, &treq->key);
				treq->offset=offset;
				treq->length=length;
				fscanf(f, "%u", &treq->crc_value);
				if(treq->key.key[0]=='d'){
					treq->value=inf_get_valueset(NULL, FS_MALLOC_W, LPAGESIZE);
				}
				else{
					treq->value=inf_get_valueset(NULL, FS_MALLOC_W, 512);			
				}
				memcpy(treq->value->value, &treq->crc_value, sizeof(uint32_t));
				break;
			case FS_DELETE_T:
				kvssd_cpy_key(&treq->temp_key, &treq->key);
				break;
			case FS_RANGEDEL_T:
				treq->offset=offset;
				kvssd_cpy_key(&treq->temp_key, &treq->key);
				break;
			case FS_RANGEGET_T:
				treq->offset=offset;
				treq->length=length;
				printf("not support!\n");
				abort();
				break;
			case FS_RMW_T:
				treq->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				treq->offset=offset;
				treq->length=length;
				printf("not support!\n");
				abort();
				break;
			default:
				break;
		}
		assign_vectored_req(req);
		if((++req_num)%100000==0){
			printf("%u\n", req_num);
		}
	}

	while(1){};
	return 1;
}

extern master_processor mp;
extern int flying_cnt;
bool first=true;
int seqcheck;
bool trace_end_req(request *const req){
	vectored_request *preq=req->parents;
	char buf[100];
	if(first){
		first=false;
		seqcheck=req->seq;
	}
	else{
		if(seqcheck+1==req->seq){
			seqcheck=req->seq;
		}
		else{
			//abort();
		}
	}
	switch(req->type){
		case FS_NOTFOUND_T:
			bench_reap_data(req, mp.li);
			inf_free_valueset(req->value,FS_MALLOC_R);
			break;
		case FS_MGET_NOTFOUND_T:
			bench_reap_data(req, mp.li);
			inf_free_valueset(req->value,FS_MALLOC_R);
			break;
		case FS_MGET_T:
		case FS_GET_T:
			map_crc_check(req->key, *(uint32_t*)req->value->value);
			bench_reap_data(req, mp.li);
			inf_free_valueset(req->value,FS_MALLOC_R);
			if(req->key.len){
				free(req->key.key);
			}
			break;
		case FS_SET_T:
			map_crc_insert(req->temp_key, req->crc_value);
			kvssd_free_key_content(&req->temp_key);
			bench_reap_data(req, mp.li);
			if(req->value) inf_free_valueset(req->value, FS_MALLOC_W);
			break;
		case FS_DELETE_T:
			map_crc_insert(req->temp_key, 0);
			kvssd_free_key_content(&req->temp_key);
			//bench_reap_data(req, mp.li);
			break;
		case FS_RMW_T:
			return true;
		case FS_TRANS_COMMIT:
			break;
		case FS_RANGEDEL_T:
			map_crc_range_delete(req);
			kvssd_free_key_content(&req->temp_key);
			break;
		case FS_RANGEGET_T:
			break;
		default:
			abort();
	}

	release_each_req(req);
	preq->done_cnt++;

	if(preq->size==preq->done_cnt){
		
		free(preq->req_array);
		free(preq);
	}

	return true;

}
