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
	while((req=get_vectored_request())){
		assign_vectored_req(req);
	}
	return NULL;
}

pthread_t thr; 
int main(int argc,char* argv[]){
	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);
	printf("signal add!\n");

	inf_init(1,0,argc,argv);

	init_koo(0);

	pthread_create(&thr, NULL, thread_test, NULL);
	pthread_join(thr, NULL);

	free_koo();
	inf_free();
	return 0;
}
