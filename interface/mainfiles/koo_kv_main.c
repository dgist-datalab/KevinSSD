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

uint32_t total_queue_size;
uint32_t send_req_size;

extern MeasureTime write_opt_time2[15];
void log_print(int sig){
	free_koo();
	inf_free();
	fflush(stdout);
	fflush(stderr);
	sync();
	exit(1);
}

void * thread_test(void *){
	vec_request *req=NULL;
	while(1){
		req=get_vectored_request();
		assign_vectored_req(req);
	}
	return NULL;
}

void log_lower_print(int sig){
	printf("-------------lower print!!!!-------------\n");
	inf_lower_log_print();
	printf("-------------lower print end-------------\n");
}

pthread_t thr; 
int main(int argc,char* argv[]){
	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);

	struct sigaction sa2;
	sa2.sa_handler = log_lower_print;
	sigaction(SIGUSR1, &sa2, NULL);

	printf("signal add!\n");
	setbuf(stdout, NULL);
	setbuf(stderr, NULL);

	inf_init(1,0,argc,argv);

	init_koo(0);
/*---------------------*
	pthread_create(&thr, NULL, thread_test, NULL);
	pthread_join(thr, NULL);
*------------thread main*/
/*-----------no thread*/
	vec_request *req=NULL;
	while((req=get_vectored_request())){
		assign_vectored_req(req);
	}
/*--------------------*/
	free_koo();
	inf_free();
	return 0;
}
