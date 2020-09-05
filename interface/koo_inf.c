#include "koo_inf.h"
#include "queue.h"
#include "../include/settings.h"
#include "interface.h"
#include "vectored_interface.h"
#include "../include/utils/kvssd.h"
#include "../bench/bench.h"
#ifdef CHECKINGDATA
#include <map>
#include <string>
#include "../include/utils/crc32.h"
using namespace std;
map<string, uint32_t> chk_data;

#endif

#define MAX_REQ_IN_TXN (2*(M/4096))

char *null_value;
queue *ack_queue;
void *ack_to_dev(void*);
bool cheeze_end_req(request *const req);
pthread_t t_id;
extern master_processor mp;

int chrfd;

static inline char *translation_buffer(char *buf, uint32_t *idx, uint32_t size, uint32_t limit);

#ifdef CHECKINGDATA
string convertToString(char *a ,int size){
	int i; 
    string s = ""; 
    for (i = 0; i < size; i++) { 
        s = s + a[i]; 
    } 
    return s; 
}

void map_crc_insert(KEYT key, char *value){
	string a=convertToString(key.key, key.len);
	chk_data.insert(pair<string, uint32_t>(a, crc32(value, LPAGESIZE)));
}

bool map_crc_check(KEYT key, char *value){
	string a=convertToString(key.key, key.len);
	uint32_t t=chk_data.find(a)->second;
	uint32_t data=crc32(value, LPAGESIZE);
	if(t!=data){
		printf("%.*s data check failed %s:%d\n", KEYFORMAT(key), __FILE__, __LINE__);
		abort();
		return false;
	}
	return true;
}

void map_crc_range_delete(KEYT key, uint32_t size){
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &key);
	for(uint32_t i=0; i<size; i++){
		(*(uint64_t*)&copied_key.key[copied_key.len-sizeof(uint64_t)])++;
		map_crc_insert(copied_key, null_value);
	}
	kvssd_free_key_content(&copied_key);
}

void map_crc_iter_check(uint32_t len, char *buf){
	uint32_t idx=0;
	KEYT key;
	while(1){
		key.len=*(uint8_t*)translation_buffer(buf, &idx, sizeof(uint16_t), len);
		key.key=translation_buffer(buf, &idx, key.len, len);
		char *value=translation_buffer(buf, &idx, LPAGESIZE, len);
		map_crc_check(key, value);
		if(idx >=len) break;
	}
}
#endif

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

void init_koo(){
	chrfd = open("/dev/cheeze_chr", O_RDWR);
	if (chrfd < 0) {
		perror("Failed to open /dev/cheeze_chr");
		abort();
		return;
	}

	null_value=(char*)malloc(PAGESIZE);
	memset(null_value,0,PAGESIZE);
	q_init(&ack_queue, 128);

	pthread_create(&t_id, NULL, &ack_to_dev, NULL);
}

static inline char *translation_buffer(char *buf, uint32_t *idx, uint32_t size, uint32_t limit){
	char *res=&buf[*idx];
	(*idx)+=size;
	if(*idx > limit){
		printf("the poiter is over limit of buffer %s:%d\n", __FILE__, __LINE__);
	}
	return res;
}
static inline void key_parser(request *req, char *buf, uint32_t *idx, uint32_t limit){
	KEYT key;
	key.len=*(uint8_t*)translation_buffer(buf, idx, sizeof(uint16_t), limit);
	key.key=translation_buffer(buf,idx,req->key.len, limit);
	kvssd_cpy_key(&req->key, &key);
}

static inline const char* type_to_str(FSTYPE t){
	switch(t){
		case FS_GET_T: return "FS_GET_T";
		case FS_SET_T: return "FS_SET_T";
		case FS_DELETE_T: return "FS_DELETE_T";
		case FS_RANGEDEL_T: return "FS_RANGEDEL_T";
		case FS_RANGEGET_T: return "FS_RANGEGET_T";
		case FS_RMW_T: return "FS_DELETE_T";
		case FS_TRANS_COMMIT: return "FS_TRANS_COMMIT";
		case FS_MGET_T: return "FS_MGET_T";
	}
	return NULL;
}

static inline void make_mget_req(vec_request *res, request *req, uint32_t size){
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->key);

	for(uint32_t i=1; i<size; i++){
		request *temp=&res->req_array[i];
		temp->tid=res->tid;
		temp->parents=res;
		temp->end_req=cheeze_end_req;
		temp->isAsync=ASYNC;
		temp->seq=i;
		temp->type==FS_MGET_T;
		kvssd_cpy_key(&temp->key, &copied_key);
		*(uint64_t*)&temp->key.key[temp->key.len-sizeof(uint64_t)]+=i;
		temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
	}

	kvssd_free_key_content(&copied_key);
}

vec_request *get_vectored_request(){
	static bool isstart=false;
	vec_request *res=(vec_request *)calloc(1, sizeof(vec_request));
	cheeze_req *creq=(cheeze_req*)malloc(sizeof(cheeze_req));

	if(!isstart){
		isstart=true;
		printf("now waiting req!!\n");
	}
	char *req_buf=(char *)malloc(2 * 1024 *1024);
	creq->buf=req_buf;
	ssize_t r=read(chrfd, creq, sizeof(cheeze_req));
	if(r<0){
		free(res);
		return NULL;
	}

	uint32_t idx=0;
	uint32_t limit=(uint32_t)creq->buf_len;
	res->origin_req=(void*)creq;
	res->tid=*(uint32_t*)translation_buffer(req_buf, &idx, sizeof(uint32_t), limit);
	res->size=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
	res->req_array=(request*)calloc(res->size, sizeof(request));
	res->end_req=NULL;
	res->mark=0;
	res->buf=req_buf;

	for(uint32_t i=0; i<res->size; i++){
		request *temp=&res->req_array[i];
		temp->tid=res->tid;
		temp->parents=res;
		temp->end_req=cheeze_end_req;
		temp->isAsync=ASYNC;
		temp->seq=i;
		temp->type=koo_to_req(*(uint8_t*)translation_buffer(req_buf, &idx, sizeof(uint8_t), limit));

		if(temp->type!=FS_TRANS_COMMIT){
			key_parser(temp, req_buf, &idx, limit);
		}

		switch(temp->type){
			case FS_GET_T:
				temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				break;
			case FS_SET_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->value=inf_get_valueset(translation_buffer(req_buf, &idx, LPAGESIZE, limit), FS_MALLOC_W, LPAGESIZE);
#ifdef CHECKINGDATA
				map_crc_insert(temp->key, temp->value->value);
#endif
				break;
			case FS_MGET_T:
				make_mget_req(res, temp, res->size);
				goto out;
			case FS_DELETE_T:
#ifdef CHECKINGDATA
				map_crc_insert(temp->key, null_value);
#endif
				break;
			case FS_RANGEDEL_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				break;
			case FS_RANGEGET_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->buf=req_buf;
#ifdef CHECKINGDATA
				map_crc_range_delete(temp->key, temp->length);
#endif
				break;
			case FS_RMW_T:
				temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->buf=translation_buffer(req_buf, &idx, temp->length, limit);
				break;
			case FS_TRANS_COMMIT:
				break;
			default:
				abort();
				break;
		}

		DPRINTF("REQ-TYPE:%s INFO(%d:%d) key:%.*s\n", type_to_str(temp->type), creq->id, i, temp->key.len, temp->key.key);
	}

out:
	return res;
}

bool cheeze_end_req(request *const req){
	vectored_request *preq=req->parents;
	switch(req->type){
		case FS_NOTFOUND_T:
			bench_reap_data(req, mp.li);
			DPRINTF("%u not found!\n",req->key);
			preq->buf_len=0;
			inf_free_valueset(req->value,FS_MALLOC_R);
			break;
		case FS_MGET_NOTFOUND_T:
			bench_reap_data(req, mp.li);
			DPRINTF("%u mget not found!\n",req->key);
			if(req->value){
				memcpy(&preq->buf[req->seq*LPAGESIZE], null_value, LPAGESIZE);
				inf_free_valueset(req->value,FS_MALLOC_R);
			}
			preq->buf_len+=LPAGESIZE;
			break;
		case FS_MGET_T:
		case FS_GET_T:
			bench_reap_data(req, mp.li);
			if(req->value){
				memcpy(&preq->buf[req->seq*LPAGESIZE], req->value->value,LPAGESIZE);
				inf_free_valueset(req->value,FS_MALLOC_R);
			}
#ifdef CHECKINGDATA
			map_crc_check(req->key, req->value->value);
#endif
			preq->buf_len+=LPAGESIZE;
			break;
		case FS_SET_T:
			bench_reap_data(req, mp.li);
			if(req->value) inf_free_valueset(req->value, FS_MALLOC_W);
			break;
		case FS_DELETE_T:
			break;
		case FS_RMW_T:
			req->type=FS_SET_T;
			memcpy(&req->value->value[req->offset], req->buf, req->length);
#ifdef CHECKINGDATA
			map_crc_insert(req->key, req->value->value);
#endif
			inf_assign_try(req);
			return true;
		case FS_TRANS_COMMIT:
			preq->buf_len=0;
			break;
		case FS_RANGEDEL_T:
#ifdef CHECKINGDATA
			map_crc_iter_check(req->buf_len, req->buf);
#endif
			break;
		case FS_RANGEGET_T:
			preq->buf_len=req->buf_len;
			break;
		default:
			abort();
	}

	release_each_req(req);
	preq->done_cnt++;

	if(preq->size==preq->done_cnt){
		cheeze_req *creq=(cheeze_req*)preq->origin_req;
		if(!creq->ret_buf){
			free(preq->buf);
			free(preq->origin_req);
			free(preq->req_array);
			free(preq);
		}
		else{
			while(!q_enqueue((void*)preq, ack_queue)){}
		}
	}

	return true;
}


void *ack_to_dev(void* a){
	vec_request *vec=NULL;
	ssize_t r;
	while(1){
		if(!(vec=(vec_request*)q_dequeue(ack_queue))) continue;

		cheeze_req *creq=(cheeze_req*)vec->origin_req;
		creq->ubuf=vec->buf;
		creq->ubuf_len=vec->buf_len;
		r=write(chrfd, creq, sizeof(cheeze_req));

		DPRINTF("[DONE] REQ INFO(%d) LBA: %u ~ %u\n", creq->id, creq->pos, creq->pos+creq->len/LPAGESIZE-1);
		if(r<0){
			printf("write error!! %s:%d\n",__FILE__, __LINE__);
			break;
		}
		free(vec->buf);
		free(vec->origin_req);
		free(vec->req_array);
		free(vec);
	}
	return NULL;
}


void free_koo(){

}
