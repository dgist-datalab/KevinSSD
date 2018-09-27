#include "../../include/container.h"
#include "../bb_checker.h"
//#include "../../lower/network/network.h"
#include "../../include/kuk_socket_lib/kuk_sock.h"
#include "../../include/utils/cond_lock.h"
#include "../interface.h"

//#define IP "127.0.0.1"
#define IP "10.42.0.2"
#define PORT 8888
#define REQSIZE (sizeof(uint64_t)*3+sizeof(uint8_t))
#define PACKETSIZE (5*REQSIZE)

struct lower_info *li;
#ifdef bdbm_drv
extern struct lower_info memio_info;
#endif
#ifdef posix_memory
extern struct lower_info my_posix;
#endif

typedef struct server_params{
	KEYT key;
	KEYT ppa;
	KEYT seq;
	value_set *value;
	struct timeval start;
	struct timeval end;
	struct timeval res;
}sp;

kuk_sock *net_worker;
queue *ret_q;
cl_lock *tflying;
cl_lock *sending;

uint32_t return_cnt;
uint64_t timeslot[100001];
static int global_value;
void *flash_returner(void *param){
	algo_req *input;
	sp *params;
	while(1){
		static int cnt=0;
		void *req;
		cl_grap(sending);
		if(!(req=(void*)q_dequeue(ret_q))){
			continue;
		}
		
		input=(algo_req*)req;
		params=(sp*)input->params;
		
		gettimeofday(&params->end,NULL);
		timersub(&params->end,&params->start,&params->res);
		uint64_t latency=params->res.tv_usec+params->res.tv_sec*1000000;
		if(latency>1000000){
			timeslot[100000]++; 
		}else{
			latency/=10;
			timeslot[latency]++;
		}   
		return_cnt++;
		
		if(++cnt%10240==0){
	//		printf("send_cnt:%d - len:%d\n",global_value++,params->seq);
		}
		kuk_send(net_worker,(char*)&params->seq,sizeof(uint32_t));
		
		free(params);
		free(input);
		cl_release(tflying);
	}
	return NULL;
}

void *al_end_req(algo_req *input){
	sp *params=(sp*)input->params;

	inf_free_valueset(params->value,input->type);

	
	while(!q_enqueue((void*)input,ret_q));
	cl_release(sending);
	return NULL;
}

algo_req *make_req(char type, KEYT key, KEYT id){
	algo_req *res=(algo_req*)malloc(sizeof(algo_req));
	sp *params=(sp*)malloc(sizeof(sp));
	params->value=inf_get_valueset(NULL,type,PAGESIZE);
	params->seq=id;
	gettimeofday(&params->start,NULL);
	res->parents=NULL;
	res->type=type;
	res->rapid=true;
	res->end_req=al_end_req;
	res->params=(void*)params;
	return res;
}

void *flash_ad(kuk_sock* ks){
	uint8_t type=*((uint8_t*)ks->p_data[0]);
	uint64_t key=*((uint64_t*)ks->p_data[1]);
	uint64_t len=*((uint64_t*)ks->p_data[2]);
	uint64_t seq=*((uint64_t*)ks->p_data[3]);
	
//	printf("recv: %d %ld %ld %ld\n",type,key,len,seq);
	for(uint64_t i=0; i<len; i++){
		static int cnt=0;
		if(++cnt%10240==0){
		//	printf("make cnt:%d\n",cnt);
		}
		cl_grap(tflying);
		algo_req *req=make_req(type,key,(uint32_t)seq);
	//	printf("key:%ld seq:%ld\n",key,seq);
		switch(type){
			case FS_GET_T:
				li->pull_data(key,PAGESIZE,((sp*)req->params)->value,ASYNC,req);
				break;
			case FS_SET_T:
				li->push_data(key,PAGESIZE,((sp*)req->params)->value,ASYNC,req);
				break;
		}
	}
	return NULL;
}
void *flash_decoder(kuk_sock *ks, void*(*ad)(kuk_sock*)){
	char **parse=ks->p_data;
	if(parse==NULL){
		parse=(char**)malloc((4+1)*sizeof(char*));
		parse[0]=(char*)malloc(sizeof(uint8_t));//type
		parse[1]=(char*)malloc(sizeof(uint64_t));//key
		parse[2]=(char*)malloc(sizeof(uint64_t));//length
		parse[3]=(char*)malloc(sizeof(uint64_t));//seq
		parse[4]=NULL;
		ks->p_data=parse;
	}

	char *dd=&ks->data[ks->data_idx];
	memcpy(parse[0],&dd[0],sizeof(uint8_t));
	memcpy(parse[1],&dd[sizeof(uint8_t)],sizeof(uint64_t));
	memcpy(parse[2],&dd[sizeof(uint8_t)+sizeof(uint64_t)],sizeof(uint64_t));
	memcpy(parse[3],&dd[REQSIZE-sizeof(uint64_t)],sizeof(uint64_t));
	
	if((*(uint8_t*)parse[0])==ENDFLAG){
		return NULL;
	}
	ks->data_idx+=REQSIZE;
	ad(ks);
	return (void*)parse;
}
int main(){
    int8_t type;
    KEYT ppa;
    algo_req *req;

#ifdef bdbm_drv
    li = &memio_info;
#endif
#ifdef posix_memory
    li = &my_posix;
#endif

	li->create(li);
	bb_checker_start(li);

	tflying=cl_init(128,false);
	sending=cl_init(128,true);

	q_init(&ret_q,1024);
	pthread_t rt_thread;
	pthread_create(&rt_thread,NULL,&flash_returner,NULL);
	
	/*
	for(int i=0; i<10*128*1024; i++){
		if(i%10240==0){
	//		printf("cnt:%d\n",i);
		}
		algo_req *req=make_req(1,i,0);
		li->push_data(i,PAGESIZE,((sp*)req->params)->value,ASYNC,req);
		cl_grap(tflying);
	}

    while(return_cnt!=10*128*1024){}
*/

	
   	net_worker=kuk_sock_init((PACKETSIZE/REQSIZE)*REQSIZE,flash_decoder,flash_ad);
	kuk_open(net_worker,IP,PORT);
	kuk_bind(net_worker);
	kuk_listen(net_worker,5);
	kuk_accept(net_worker);

	int len=0;
	while((len=kuk_recv(net_worker,net_worker->data,net_worker->data_size))){
		net_worker->data_idx=0;
		while(len!=net_worker->data_idx){
			net_worker->decoder(net_worker,net_worker->after_decode);
		}
	}


	for(int i=0; i<100000; i++){
		if(timeslot[i])
			printf("%d\t%ld\n",i*10,timeslot[i]);
	}
}
