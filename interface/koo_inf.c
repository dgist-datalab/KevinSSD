#include "koo_inf.h"
#include "queue.h"
#include "../include/settings.h"
#include "interface.h"
#include "vectored_interface.h"
#include "../include/utils/kvssd.h"
#include "../bench/bench.h"
#include "../include/settings.h"
#include "../include/sem_lock.h"
#ifdef CHECKINGDATA
#include <map>
#include <string>
#include "../include/utils/crc32.h"
using namespace std;
map<string, uint32_t> chk_data;

#endif


uint32_t iteration_cnt;
char* debug_koo_key="look.1.gz";

#define MAX_REQ_IN_TXN (2*(M/4096))

char *null_value;
queue *ack_queue;
void *ack_to_dev(void*);
bool cheeze_end_req(request *const req);
void print_buf(char *buf, uint32_t len);
pthread_t t_id;
extern master_processor mp;
fdriver_lock_t  crc_lock;

void print_key(KEYT key, bool data_print){
	if(key.len==0 || (!data_print && key.key[0]=='d')){
		printf("\n");
		return;
	}
	uint64_t block_num=(*(uint64_t*)&key.key[1]);
	block_num=Swap8Bytes(block_num);
	uint32_t offset=1+sizeof(uint64_t);
	if(key.key[0]=='m'){
		uint32_t remain=key.len-offset;
		DPRINTF("key: %c %lu %.*s(keylen:%u)\n",key.key[0],block_num,remain,&key.key[offset], remain);
	}
	else{
		uint64_t block_num2=*(uint64_t*)&key.key[offset];
		block_num2=Swap8Bytes(block_num2);
		DPRINTF("key: %c %lu bn:%lu, %d\n",key.key[0],block_num, block_num2, key.key[offset]);
	}
}

bool checking_filename(KEYT key, char *s){
	if(key.len < 9) return false;
	KEYT temp;
	temp.key=&key.key[9];
	temp.len=key.len-9-1;
	return KEYCONSTCOMP(temp, s)==0;
}

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

void map_crc_insert(KEYT key, char *value, uint32_t length){
	fdriver_lock(&crc_lock);
	string a=convertToString(key.key, key.len);
	map<string, uint32_t>::iterator it=chk_data.find(a);
	if(it!=chk_data.end()){
		if(key.key[0]=='d'){
			printf("data update????\n");
			sleep(1);
		}
		it->second=crc32(value,length);
	}
	else{
		chk_data.insert(pair<string, uint32_t>(a, crc32(value, LPAGESIZE)));
	}
	fdriver_unlock(&crc_lock);
}

bool map_crc_check(KEYT key, char *value, uint32_t length){
	fdriver_lock(&crc_lock);
	string a=convertToString(key.key, key.len);
	map<string, uint32_t>::iterator it=chk_data.find(a);
	if(it==chk_data.end()){
		printf("not populated key!!\n");
		print_key(key, true);
		fdriver_unlock(&crc_lock);
		return true;
	}

	uint32_t t=it->second;
	uint32_t data=crc32(value, length);

	if(t!=data){
		printf("data check failed");
		print_key(key, true);
	//	abort();
		fdriver_unlock(&crc_lock);
		return false;
	}
	fdriver_unlock(&crc_lock);
	return true;
}

void map_crc_range_delete(KEYT key, uint32_t size){
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &key);
	for(uint32_t i=0; i<size; i++){
		(*(uint64_t*)&copied_key.key[copied_key.len-sizeof(uint64_t)])++;
		map_crc_insert(copied_key, null_value, LPAGESIZE);
	}
	kvssd_free_key_content(&copied_key);
}

void map_crc_iter_check(uint32_t len, char *buf){
	uint32_t idx=0;
	KEYT key;
	printf("not implemented!!\n");
	abort();
	while(1){
		key.len=*(uint8_t*)translation_buffer(buf, &idx, sizeof(uint16_t), len);
		key.key=translation_buffer(buf, &idx, key.len, len);
		char *value=translation_buffer(buf, &idx, LPAGESIZE, len);
		//map_crc_check(key, value);
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
#ifdef DEBUG
	printf("Debugging mode on!\n");
#endif

#ifdef CHECKINGDATA
	fdriver_mutex_init(&crc_lock);
	printf("Data checking mode on!\n");
#endif

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
	uint16_t key_len=*(uint16_t*)translation_buffer(buf, idx, sizeof(uint16_t), limit);
	key.len=key_len;
	key.key=translation_buffer(buf,idx,req->key.len, limit);
	kvssd_cpy_key(&req->key, &key);
	(*idx)+=key.len;
}

static inline void error_check(request *req){
	if(req->key.len){
		if(!(req->key.key[0]=='d' || req->key.key[0]=='m')){
			printf("key error %s:%d\n",__FILE__, __LINE__);
		}
	}
	else if(req->type!=FS_TRANS_COMMIT){
		printf("not commit but no key %s:%d\n", __FILE__, __LINE__);
		abort();
	}
}

static inline const char* type_to_str(FSTYPE t){
	switch(t){
		case FS_GET_T: return "FS_GET_T";
		case FS_SET_T: return "FS_SET_T";
		case FS_DELETE_T: return "FS_DELETE_T";
		case FS_RANGEDEL_T: return "FS_RANGEDEL_T";
		case FS_RANGEGET_T: return "FS_RANGEGET_T";
		case FS_RMW_T: return "FS_RMW_T";
		case FS_TRANS_COMMIT: return "FS_TRANS_COMMIT";
		case FS_MGET_T: return "FS_MGET_T";
	}
	return NULL;
}

static inline void make_mget_req(vec_request *res, request *req, uint32_t size){
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->key);

	for(uint32_t i=0; i<size; i++){
		request *temp=&res->req_array[i];
		temp->tid=res->tid;
		temp->parents=res;
		temp->end_req=cheeze_end_req;
		temp->isAsync=ASYNC;
		temp->seq=i;
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

void print_buf(char *buf, uint32_t len){
	for(uint32_t i=1; i<=len; i++){
		printf("%d ",buf[i-1]);
		if(i%10==0){
			printf("\n");
		}
	}
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
	res->eof=0;

	//print_buf(res->buf, 100);

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
				if(temp->key.key[0]=='d'){
					temp->value=inf_get_valueset(translation_buffer(req_buf, &idx, LPAGESIZE, limit), FS_MALLOC_W, LPAGESIZE);
				}
				else{
					temp->value=inf_get_valueset(translation_buffer(req_buf, &idx, LPAGESIZE, limit), FS_MALLOC_W, 512);			
				}
#ifdef CHECKINGDATA
	//			map_crc_insert(temp->key, temp->value->value);
#endif
				break;
			case FS_MGET_T:
				make_mget_req(res, temp, res->size);
				goto out;
			case FS_DELETE_T:
#ifdef CHECKINGDATA
	//			map_crc_insert(temp->key, null_value);
#endif
				break;
			case FS_RANGEDEL_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
#ifdef CHECKINGDATA
				map_crc_range_delete(temp->key, temp->length);
#endif
				break;
			case FS_RANGEGET_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->buf=req_buf;
				iteration_cnt++;
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

		error_check(temp);

#ifdef DEBUG
		if(temp->type==FS_RANGEGET_T){
			DPRINTF("TID: %u REQ-TYPE:%s (%s) INFO(seq-%d:%d, ret_buf:%px) (keylen:%d) ",temp->tid, type_to_str(temp->type), temp->offset?"from":"next",creq->id, i, creq->ret_buf, temp->key.len);
			print_key(temp->key, true);
		}
		else{
	//		if(temp->type!=FS_SET_T && temp->type!=FS_TRANS_COMMIT){
				DPRINTF("TID: %uREQ-TYPE:%s INFO(seq-%d:%d, ret_buf:%px) (keylen:%d)",temp->tid, type_to_str(temp->type), creq->id, i, creq->ret_buf, temp->key.len);
				print_key(temp->key, true);
	//		}
		}
#endif
		/*
		if(print_value && temp->type==FS_SET_T){
			printf("meta set value print\n");
			//print_buf(temp->value->value, 4096);
		}*/
	}

out:
	return res;
}

bool cheeze_end_req(request *const req){
	vectored_request *preq=req->parents;
	switch(req->type){
		case FS_NOTFOUND_T:
			bench_reap_data(req, mp.li);
#ifdef DEBUG
			DPRINTF("\t\t%d-not found!",0);
			print_key(req->key, true);
#ifdef CHECKINGDATA
			if(map_crc_check(req->key, null_value, LPAGESIZE)){
				printf("\t\tdeleted key!! or not inserted key\n");
				abort();
			}
			else{
			//	printf("\t\tPinK really can't find it\n");
			//	abort();
			}
#endif

#endif
			preq->buf_len=0;
			inf_free_valueset(req->value,FS_MALLOC_R);
			break;
		case FS_MGET_NOTFOUND_T:
			bench_reap_data(req, mp.li);
#ifdef DEBUG
			DPRINTF("\t\t%d-not mget found!",0);
			print_key(req->key, true);

#ifdef CHECKINGDATA
			if(map_crc_check(req->key, null_value, LPAGESIZE)){
				printf("\t\tdeleted key!! or not inserted key\n");
				abort();
			}
			else{
			//	printf("\t\tPinK really can't find it\n");
			//	abort();
			}
#endif
			preq->eof=1;
#endif
			if(req->value){
				memcpy(&preq->buf[req->seq*LPAGESIZE], null_value, LPAGESIZE);
				inf_free_valueset(req->value,FS_MALLOC_R);
			}
			preq->buf_len+=LPAGESIZE;
			break;
		case FS_MGET_T:
		case FS_GET_T:
			/*
			printf("get all value print\n");
			print_buf(req->value->value, 4096);
			*/
			bench_reap_data(req, mp.li);
#ifdef CHECKINGDATA
			if(!map_crc_check(req->key, req->value->value, req->value->length)){
				printf("data missed!\n");
				//abort();
			}
#endif
			if(req->value){
				memcpy(&preq->buf[req->seq*LPAGESIZE], req->value->value,req->value->length);
				inf_free_valueset(req->value,FS_MALLOC_R);
			}

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
			map_crc_insert(req->key, req->value->value, req->value->length);
#endif
			inf_assign_try(req);
			return true;
		case FS_TRANS_COMMIT:
			*(uint8_t*)&preq->buf[0]=1;
			preq->buf_len=1;
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
	//		DPRINTF("[DONE] REQ INFO(%d) type:%s ret_buf:%p ubuf:%p ubuf_len:%d\n", creq->id, type_to_str(preq->req_array[0].type), creq->ret_buf, creq->ubuf, creq->ubuf_len);
			free(preq->buf);
			free(preq->origin_req);
			free(preq->req_array);
			free(preq);
		}
		else{
			if(req->type==FS_RANGEGET_T){
				(*(uint16_t*)&preq->buf[preq->buf_len])=preq->eof;
				preq->buf_len+=sizeof(uint16_t);
			}
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
		creq->ubuf_len=(creq->ubuf_len==0?vec->buf_len:creq->ubuf_len);
		if(vec->req_array[0].type==FS_NOTFOUND_T){
			creq->ubuf_len=0;
		}
		r=write(chrfd, creq, sizeof(cheeze_req));
#ifdef DEBUG	
	//	DPRINTF("[DONE] REQ INFO(%d) ret_buf:%p ubuf:%p ubuf_len:%d\n", creq->id, creq->ret_buf, creq->ubuf, creq->ubuf_len);
		if(vec->req_array[0].type==FS_RANGEGET_T){
			printf("[DONE] REQ INFO(%d) type:%s ret_buf:%p ubuf:%p ubuf_len:%d more?:%d\n", creq->id, type_to_str(vec->req_array[0].type), creq->ret_buf, creq->ubuf, creq->ubuf_len,vec->eof);
		}
		else if(vec->req_array[0].type==FS_MGET_T){
			printf("[DONE] REQ INFO(%d) type:%s ret_buf:%p ubuf:%p ubuf_len:%d size?:%d suc?:%d\n", creq->id, type_to_str(vec->req_array[0].type), creq->ret_buf, creq->ubuf, creq->ubuf_len, vec->size, !vec->eof);
		}
		else if(vec->req_array[0].type!=FS_TRANS_COMMIT){
			printf("[DONE] REQ INFO(%d) type:%s ret_buf:%p ubuf:%p ubuf_len:%d\n", creq->id, type_to_str(vec->req_array[0].type), creq->ret_buf, creq->ubuf, creq->ubuf_len);
		}
#endif
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
	printf("\t\titeration number:%u\n", iteration_cnt);
}
