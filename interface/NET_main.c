#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>

#include "../include/settings.h"
#include "../bench/bench.h"
#include "../include/flash_sock/fd_sock.h"
#include "../include/flash_sock/buffer_manager.h"
#include "interface.h"
#include "queue.h"
fd_sock_manager *net;
queue *n_q;
int protocol_type(char *);
int input_num, send_num;
MeasureTime write_opt_time[10];
void log_print(int sig){
	fd_sock_clear(net);
	inf_free();
	exit(1);
}

void *ack_to_client(void *arg){
	netdata *net_data;
	while(1){
		void *req;
		req=q_dequeue(n_q);
		if(!req) continue;
		net_data=(netdata*)req;
		fd_sock_reply(net,net_data);
		free(net_data);
		send_num++;
	}
}

void kv_main_end_req(uint32_t a, uint32_t b, void *req){
	if(req==NULL) return;
	netdata *net_data=(netdata*)req;
	switch(net_data->type){
		case FS_RANGEGET_T:
		case FS_GET_T:
			while(!q_enqueue((void*)net_data,n_q));
			break;
		case FS_SET_T:
			break;
	}
}

int main(int argc, char *argv[]){
	if(argc!=2){
		printf("please input the protocol type {YCSB,REDIS,ROCKSDB,OLTP}\n");
		exit(1);
	}
	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);
	printf("signal add!\n");

	q_init(&n_q,128);
	
	net=fd_sock_init(IP,PORT,protocol_type(argv[1]));

	pthread_t t_id;
	pthread_create(&t_id,NULL,ack_to_client,NULL);
	inf_init(1,0);

	char temp[8192]={0,};
	netdata *data;
	bool early_reply=false;
	while(1){
		data=(netdata*)malloc(sizeof(netdata));
		early_reply=fd_sock_requests(net,data);
		switch(data->type){
			case WRITE_TYPE:
			case READ_TYPE:
				inf_make_req_apps(data->type,data->key,data->keylen,temp,PAGESIZE-data->keylen-sizeof(data->keylen),data->seq,data->type==2?data:NULL,kv_main_end_req);
				break;
			case RANGE_TYPE:
				inf_make_range_query_apps(data->type,data->key,data->keylen,data->seq,data->scanlength,data,kv_main_end_req);
				break;
		}
		if(early_reply){
			while(!q_enqueue((void*)data,n_q));
		}
	}

	while(!bench_is_finish()){}
}
