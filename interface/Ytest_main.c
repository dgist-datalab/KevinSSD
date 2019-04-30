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
#include "interface.h"
#include "queue.h"

typedef struct netdata_t{
	uint32_t type;
	uint8_t keylen;
	uint32_t scanlength;
	uint32_t seq;
	char key[UINT8_MAX];
}netdata;
MeasureTime temp;
int client_socket;
queue *n_q;

void log_print(int sig){
	inf_free();
	exit(1);
}


int server_socket;
struct sockaddr_in client_addr;
socklen_t client_addr_size;

void read_socket_len(char *buf, int len){
	int readed=0, temp_len;
	while(len!=readed){
		if((temp_len=read(client_socket,buf,len-readed))<0){
			if(errno==11) continue;
			printf("errno: %d temp_len:%d\n",errno,temp_len);
			printf("length read error!\n");
			abort();
		}else if(temp_len==0){
			printf("connection closed :%d\n", errno);
			client_socket     = accept( server_socket, (struct sockaddr*)&client_addr, &client_addr_size);

		}
		readed+=temp_len;
	}
}

void write_socket_len(char *buf, int len){
	int wrote=0, temp_len;
	while(len!=wrote){
		if((temp_len=write(client_socket,buf,len-wrote))<0){
			if(errno==11) continue;
			printf("errno: %d\n",errno);
			printf("length write error!\n");
			abort();
		}
		wrote+=temp_len;
	}
}
void print_byte(char *data, int len){
	for(int i=0; i<len; i++){
		printf("%d ",data[i]);
	}
	printf("\n");
}

void *ack_to_client(void *arg){
	netdata *net_data;
	while(1){
		void *req;
		req=q_dequeue(n_q);
		if(!req) continue;
		net_data=(netdata*)req;
		//printf("write:");
		//print_byte((char*)&net_data->seq,sizeof(net_data->seq));
		write_socket_len((char*)&net_data->seq,sizeof(net_data->seq));
	//	if(net_data->type==2){
			free(net_data);
	//	}
	}
}

void kv_main_end_req(uint32_t a, uint32_t b, void *req){
	if(req==NULL) return;
	netdata *net_data=(netdata*)req;
//	net_data->seq=a;
	switch(net_data->type){
		case FS_GET_T:
			//printf("insert_queue\n");
	//		while(!q_enqueue((void*)net_data,n_q));
	//		printf("assign seq:%d\n",a);
			free(net_data);
			break;
		case FS_RANGEGET_T:
		case FS_SET_T:
			free(net_data);
			break;
	}
}

MeasureTime write_opt_time[10];
MeasureTime temp_time;
int main(int argc, char *argv[]){
	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);
	printf("signal add!\n");

	if(argc<2){
//		printf("insert argumen!\n");
//		return 1;
	}
	inf_init(1,1000000);
	FILE *fp = fopen("ycsb_load_gc", "r");
	netdata *data;
	char temp[8192]={0,};
	char data_temp[6];
	data=(netdata*)malloc(sizeof(netdata));
	static int cnt=0;
	static int req_cnt=0;
	//measure_init(&data->temp);
	
	bench_custom_init(write_opt_time,10);
	bench_custom_start(write_opt_time,0);
	while((fscanf(fp,"%d %d %d %s",&data->type,&data->scanlength,&data->keylen,data->key))!=EOF){
		if(data->type==1 || data->type==2){
			//printf("%d %d %.*s\n",data->type,data->keylen,data->keylen,data->key);
		    inf_make_req_apps(data->type,data->key,data->keylen,temp,PAGESIZE-data->keylen-sizeof(data->keylen),cnt++,data,kv_main_end_req);	
		}
		else{
			data->type=FS_RANGEGET_T;
			inf_make_range_query_apps(data->type,data->key,data->keylen,cnt++,data->scanlength,data,kv_main_end_req);
		}
		data=(netdata*)malloc(sizeof(netdata));
		if(req_cnt++%10240==0){
			printf("cnt:%d\n",req_cnt);
		}
	}
	while(!bench_is_finish()){}
	bench_custom_A(write_opt_time,0);
	inf_free();
	bench_custom_print(write_opt_time,10);

}
