#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <limits.h>
#include <getopt.h>
#include "../include/lsm_settings.h"
#include "../include/FS.h"
#include "../include/settings.h"
#include "../include/types.h"
#include "../bench/bench.h"
#include "interface.h"
#include "vectored_interface.h"
#include "../algorithm/Lsmtree/lsmtree.h"
#include "../include/utils/kvssd.h"
extern int req_cnt_test;
extern uint64_t dm_intr_cnt;
extern int LOCALITY;
extern float TARGETRATIO;
extern int KEYLENGTH;
extern int VALUESIZE;
extern uint32_t INPUTREQNUM;
extern master *_master;
extern bool force_write_start;
extern int seq_padding_opt;
extern lsmtree LSM;
extern int v_cnt[NPCINPAGE+1];
#ifdef Lsmtree
int skiplist_hit;
#endif
MeasureTime write_opt_time[11];
extern master_processor mp;
extern uint64_t cumulative_type_cnt[LREQ_TYPE_NUM];
MeasureTime total_time;
KEYT debug_key={40,"0000000000000000000000000000000000000009"};
void log_print(int sig){
//	while(!bench_is_finish()){}
	inf_free();
	exit(1);
}

int main(int argc,char* argv[]){

	struct sigaction sa;
	sa.sa_handler = log_print;
	sigaction(SIGINT, &sa, NULL);
	printf("signal add!\n");

	char *temp_argv[10];
	int temp_cnt=bench_set_params(argc,argv,temp_argv);
	inf_init(0,0,temp_cnt,temp_argv);
	bench_init();
	bench_vectored_configure();
	bench_transaction_configure(2, 1);
	printf("TOTALKEYNUM: %ld\n",TOTALKEYNUM);
	//bench_add(VECTOREDUNIQRSET,0,(INPUTREQNUM?INPUTREQNUM:SHOWINGFULL)/1,((INPUTREQNUM?INPUTREQNUM:SHOWINGFULL)));
	bench_add(VECTOREDRW,0,(INPUTREQNUM?INPUTREQNUM:SHOWINGFULL)/1,((INPUTREQNUM?INPUTREQNUM:SHOWINGFULL)));
	//bench_add(VECTOREDMIXED,0,(INPUTREQNUM?INPUTREQNUM:SHOWINGFULL)/1,((INPUTREQNUM?INPUTREQNUM:SHOWINGFULL))/2);

	char *value;
	uint32_t mark;
	uint32_t i=0; 
	while((value=get_vectored_bench(&mark, true))){
		inf_vector_make_req(value, bench_transaction_end_req, mark);
		inf_vector_make_req(get_vectored_one_command(FS_RANGEGET_T, 3000, 10*i++), bench_transaction_end_req, -1);
	}

	/*
	for(uint32_t i=1; i<=SHOWINGFULL; i++){
		inf_vector_make_req(get_vectored_one_command(FS_DELETE_T, 3000+i/512, rand()%SHOWINGFULL), bench_transaction_end_req, -1);
		if(i%512==0){
			inf_vector_make_req(get_vectored_one_command(FS_TRANS_COMMIT, 3000+i/512-1, UINT32_MAX), bench_transaction_end_req, -1);
		}
	}
	*/
	//while(1){
//
//	}

	force_write_start=true;
	
	printf("bench finish\n");
	while(!bench_is_finish()){
#ifdef LEAKCHECK
		sleep(1);
#endif
	}
	inf_free();

	return 0;
}
