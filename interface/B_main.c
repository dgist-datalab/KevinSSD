#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include<signal.h>
#include "../include/lsm_settings.h"
#include "../include/FS.h"
#include "../include/settings.h"
#include "../include/types.h"
#include "../bench/bench.h"
#include "server.h"
#include "interface.h"
#include "queue.h"

#include "../include/kuk_socket_lib/kuk_sock.h"
#ifdef Lsmtree
int skiplist_hit;
#endif
kuk_sock *net_worker;
//#define IP "127.0.0.1"
#define REQSIZE sizeof(net_data_t)
#define PACKETSIZE sizeof(net_data_t)
queue *ret_q;
pthread_mutex_t send_lock;

pthread_mutex_t ret_q_lock;
pthread_cond_t ret_cond;

static int global_value;
void log_print(int sig){
	inf_free();
	exit(1);
}
void *flash_returner(void *param){
	while(1){
		static int cnt=0;
		void *req;
		if(!(req=q_dequeue(ret_q))){
			continue;
		}

		if((*(int*)req)!=0){	
			pthread_mutex_lock(&send_lock);
			if(++cnt%10240==0){
				printf("send_cnt:%d - len:%d\n",global_value++,*(int*)req);
			}
			//printf("seq:%u",*(int*)req);
			kuk_send(net_worker,(char*)req,sizeof(uint32_t));
			pthread_mutex_unlock(&send_lock);
		}
		free(req);
	}
	return NULL;
}

void *flash_ack2clnt(void *param){
	void **params=(void**)param;
	uint8_t type=*((uint8_t*)params[0]);
	//uint32_t *tt;
	//uint32_t seq=*((uint32_t*)params[1]);
	switch(type){
		case FS_NOTFOUND_T:
		case FS_GET_T:
			/*
			kuk_ack2clnt(net_worker);*/
			//kuk_send(net_worker,(char*)&seq,sizeof(seq));
			while(!q_enqueue((void*)params[1],ret_q)){}
			break;
			/*
		case FS_SET_T:
			tt=(uint32_t*)malloc(sizeof(uint32_t));
			*tt=UINT_MAX;
			while(!q_enqueue((void*)tt,ret_q))
			free(params[1]);
			break;*/
		default:
			break;
	}

	free(params[0]);
	//free(params[1]);
	free(params);
	return NULL;
}

void *flash_ad(kuk_sock* ks){
	uint8_t type=*((uint8_t*)ks->p_data[0]);
	uint64_t key=*((uint64_t*)ks->p_data[1]);
	uint64_t len=*((uint64_t*)ks->p_data[2]);
	uint32_t seq=*((uint32_t*)ks->p_data[3]);

	printf("type:%d key:%lu len:%lu seq:%u\n",type,key,len,seq);
	char t_value[PAGESIZE];
	memset(t_value,'x',PAGESIZE);

	value_set temp;
	temp.value=t_value;
	temp.dmatag=-1;
	temp.length=PAGESIZE;
	
	for(uint64_t i=0; i<len; i++){
		static int cnt=0;
		if(i+1!=len){
			inf_make_req_special(type,(uint32_t)key+i,&temp,0,flash_ack2clnt);
		}else{
			inf_make_req_special(type,(uint32_t)key+i,&temp,seq,flash_ack2clnt);
		}
	}
	return NULL;
}

void *flash_decoder(kuk_sock *ks, void*(*ad)(kuk_sock*)){
	char **parse=ks->p_data;
	if(parse==NULL){
		
		/*parse=(char**)malloc((4+1)*sizeof(char*));
		parse[0]=(char*)malloc(sizeof(uint8_t));//type
		parse[1]=(char*)malloc(sizeof(uint64_t));//key
		parse[2]=(char*)malloc(sizeof(uint64_t));//length
		parse[3]=(char*)malloc(sizeof(uint32_t));//seq
		parse[4]=NULL;*/
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

int main(int argc,char* argv[]){
	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);

	inf_init();
	char t_value[PAGESIZE];
	memset(t_value,'x',PAGESIZE);
	bench_init(1);
	bench_add(NOR,0,-1,-1);
	
	q_init(&ret_q,1024);
	pthread_t rt_thread;
	pthread_create(&rt_thread,NULL,&flash_returner,NULL);
	value_set dummy;
	dummy.value=t_value;
	dummy.dmatag=-1;
	dummy.length=PAGESIZE;
	static int cnt=0;
	/*network initialize*/
#ifdef NETWORKSET
	net_worker=kuk_sock_init(sizeof(net_data_t),flash_decoder,flash_ad);
	kuk_open(net_worker,IP,PORT);
	kuk_bind(net_worker);
	kuk_listen(net_worker,5);
	kuk_accept(net_worker);
	printf("connected......\n");
	uint32_t readed=0, len=0;

	net_data_t temp;
	while (1) {
		while (readed == 0 || readed % REQSIZE != 0) {
			len = kuk_recv(net_worker, &((char*)&temp)[readed], sizeof(net_data_t)-readed);
			if (len == -1) continue;
			readed += len;
		}
		printf("%d %lu %lu %u %d\n",temp.type, temp.offset, temp.len,temp.seq,cnt++);
		if((temp.type!=1 && temp.type!=2) || temp.len!=1){
			printf("no");
		}
		readed=0;
		net_worker->data_idx=0;

		for(int i=0; i<temp.len; i++){
			if(i+1!=temp.len){
				inf_make_req_special(temp.type,(uint32_t)temp.offset+i,&dummy,0,flash_ack2clnt);
			}else{
				inf_make_req_special(temp.type,(uint32_t)temp.offset+i,&dummy,temp.seq,flash_ack2clnt);
			}
		}
	}
#else
	int a;
	uint64_t b,c,d;
	value_set temp;
	temp.value=t_value;
	temp.dmatag=-1;
	temp.length=PAGESIZE;
	MeasureTime t;
	MS(&t);
	//int cnt=0;
	int read_cnt=0;
	while(read_cnt++<2132648){

		scanf("%d%ld%ld\n",&a,&b,&c);
		for(uint64_t i=0; i<c; i++){
			cnt++;
			if(cnt%10240==0){
				printf("cnt:%d\n",cnt);
			}
			inf_make_req(a,b+i,&temp,0);
		}
	}
	MT(&t);
#endif
	kuk_sock_destroy(net_worker);
	return 0;
}
