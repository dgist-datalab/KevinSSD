#include "bench.h"
#include "../include/settings.h"
extern master *_master;

#define MAXBUFSIZE (16*K)
#define REQSIZE (sizeof(uint8_t)+sizeof(uint8_t)+sizeof(uint32_t)) //except length of key
#define TXNHEADERSIZE (sizeof(uint32_t)+sizeof(uint32_t))

extern int KEYLENGTH;

void bench_transaction_configure(uint32_t commit_term, uint32_t
		transaction_size){
	_master->trans_configure.commit_term=commit_term;
	_master->trans_configure.transaction_size=transaction_size;
	_master->trans_configure.request_size=REQSIZE+KEYLENGTH*16-sizeof(ppa_t);
	_master->trans_configure.request_num_per_command=(MAXBUFSIZE-TXNHEADERSIZE)/_master->trans_configure.request_size;
}

char *transaction_begin_req(uint32_t tid){
	uint32_t buf_size=sizeof(uint32_t)*2+sizeof(uint8_t);
	char *res=(char*)malloc(buf_size);
	*(uint32_t*)&res[0]=tid;
	*(uint32_t*)&res[sizeof(uint32_t)]=1;
	*(uint8_t*)&res[buf_size-sizeof(uint8_t)]=FS_TRANS_BEGIN;
	return res;
}

char *transaction_commit_req(int *tid_list, uint32_t size){
	uint32_t buf_size=sizeof(uint32_t)*2+(sizeof(uint8_t)+sizeof(uint32_t))*size;
	char *res=(char*)malloc(buf_size);
	*(uint32_t*)&res[0]=tid_list[0];
	*(uint32_t*)&res[sizeof(uint32_t)]=size;
	uint32_t idx=sizeof(uint32_t)*2;

	for(uint32_t i=0; i<size; i++){
		*(uint8_t*)&res[idx++]=FS_TRANS_COMMIT;
		*(int32_t*)&res[idx]=tid_list[i];
		idx+=sizeof(uint32_t);
	}
	return res;
}

char *get_transaction_bench(uint32_t *mark){
	static int idx=0, prev=-1;
	static int tid_buf[16]={0,};
	static int transaction_id=0;
	static int now_transaction_req_cnt=0;
	static uint64_t real_req_num=0;

	
	monitor *m=&_master->m[_master->n_num];
	*mark=_master->n_num;

	if(idx!=0 && idx%_master->trans_configure.commit_term == 0){
		char *commit_res=transaction_commit_req(tid_buf, idx);
		idx=0;
		prev=-1;
		return commit_res;
	}


	if(m->command_issue_num==0){ //start bench mark
		bench_make_data();
		real_req_num=0;
	}


	if(m->command_issue_num==m->command_num){
		_master->n_num++;
		free(m->tbody);
		while(!bench_is_finish_n(_master->n_num)){}
		printf("\rtesting...... [100%%] done!\n");
		printf("\n");

		if(_master->n_num==_master->m_num) return NULL;

		return transaction_commit_req(tid_buf, idx);
	}


	if(prev!=idx){
		prev++;
		m->command_issue_num++;
		m->command_num++;
		return transaction_begin_req(transaction_id);
	}

#ifdef PROGRESS
	if(m->command_issue_num % (m->command_num/100)==0){
		printf("\r testing...... [%.2lf%%]",(double)(m->command_issue_num)/(m->command_num/100));
		fflush(stdout);
	}
#endif


	transaction_bench_value *res;
	res=&m->tbody[real_req_num++];
	*(uint32_t*)&res->buf[0]=transaction_id;

	now_transaction_req_cnt++;

	if(now_transaction_req_cnt  >= _master->trans_configure.transaction_size){
		prev=idx;
		tid_buf[idx++]=transaction_id++; //next start new transaction;
		now_transaction_req_cnt=0;
	}

	m->command_issue_num++;
	m->n_num++;
	return res->buf;
}

void trans_set(uint32_t start, uint32_t end, monitor* m, bool isseq){
	uint32_t request_per_command=_master->trans_configure.request_num_per_command;
	uint32_t number_of_command=(m->m_num)/request_per_command;
	m->m_num=number_of_command*request_per_command;
	m->tbody=(transaction_bench_value*)malloc(number_of_command * sizeof(transaction_bench_value));

	uint32_t request_buf_size=_master->trans_configure.request_size * request_per_command;

	m->command_num=number_of_command;
	m->command_issue_num=0;
	printf("total command : %u\n", m->command_num);
	for(uint32_t i=0; i<number_of_command; i++){
		uint32_t idx=0;
		m->tbody[i].buf=(char*)malloc(request_buf_size + TXNHEADERSIZE);
		char *buf=m->tbody[i].buf;

		idx+=sizeof(uint32_t);//tid
		(*(uint32_t*)&buf[idx])=request_per_command;
		idx+=sizeof(uint32_t);

		for(uint32_t j=0; j<request_per_command; j++){
			(*(uint8_t*)&buf[idx])=FS_SET_T;
			idx+=sizeof(uint8_t);

			(*(uint8_t*)&buf[idx])=KEYLENGTH*16-sizeof(uint32_t);
			idx+=sizeof(uint8_t);
			if(isseq){	
				idx+=my_itoa(start+i*request_per_command+j, NULL, &buf[idx]);
			}
			else{
				idx+=my_itoa(start+rand()%(end-start), NULL, &buf[idx]);
			}
			(*(uint32_t*)&buf[idx])=0; //offset
			idx+=sizeof(uint32_t);
			m->write_cnt++;
			if(idx > request_buf_size+TXNHEADERSIZE){
				printf("%s:%d bufffer overflow!\n", __FILE__,__LINE__);
				abort();		
			}
		}
	}
}

void trans_get(uint32_t start, uint32_t end, monitor* m, bool isseq){
	uint32_t request_per_command=_master->trans_configure.request_num_per_command;
	uint32_t number_of_command=(m->m_num)/request_per_command;
	m->m_num=number_of_command*request_per_command;
	m->tbody=(transaction_bench_value*)malloc(number_of_command * sizeof(transaction_bench_value));

	uint32_t request_buf_size=_master->trans_configure.request_size * request_per_command;
	m->command_num=number_of_command;
	m->command_issue_num=0;
	for(uint32_t i=0; i<number_of_command; i++){
		uint32_t idx=0;
		m->tbody[i].buf=(char*)malloc(request_buf_size + TXNHEADERSIZE);
		char *buf=m->tbody[i].buf;

		idx+=sizeof(uint32_t);//tid
		(*(uint32_t*)&buf[idx])=request_per_command;
		idx+=sizeof(uint32_t);

		for(uint32_t j=0; j<request_per_command; j++){
			(*(uint8_t*)&buf[idx])=FS_GET_T;
			idx+=sizeof(uint8_t);

			(*(uint8_t*)&buf[idx])=KEYLENGTH*16-sizeof(uint32_t);
			idx+=sizeof(uint8_t);
			if(isseq){	
				idx+=my_itoa(start+i*request_per_command+j, NULL, &buf[idx]);
			}
			else{
				idx+=my_itoa(start+rand()%(end-start), NULL, &buf[idx]);
			}
			(*(uint32_t*)&buf[idx])=0; //offset
			idx+=sizeof(uint32_t);
			m->read_cnt++;
		}
	}
}

void trans_rw(uint32_t start, uint32_t end, monitor* m, bool isseq){
	uint32_t request_per_command=_master->trans_configure.request_num_per_command;
	uint32_t number_of_command=(m->m_num)/request_per_command;
	m->m_num=number_of_command*request_per_command;
	m->tbody=(transaction_bench_value*)malloc(number_of_command * sizeof(transaction_bench_value));

	uint32_t request_buf_size=_master->trans_configure.request_size * request_per_command;
	int *key_buf=(int*)malloc(sizeof(int) * request_per_command);
	m->command_num=number_of_command;
	m->command_issue_num=0;

	for(uint32_t i=0; i<number_of_command/2; i++){
		uint32_t idx=0;
		m->tbody[i].buf=(char*)malloc(request_buf_size + TXNHEADERSIZE);
		char *buf=m->tbody[i].buf;

		idx+=sizeof(uint32_t);//tid
		(*(uint32_t*)&buf[idx])=request_per_command;
		idx+=sizeof(uint32_t);

		for(uint32_t j=0; j<request_per_command; j++){
			(*(uint8_t*)&buf[idx])=FS_SET_T;
			idx+=sizeof(uint8_t);

			(*(uint8_t*)&buf[idx])=KEYLENGTH*16-sizeof(uint32_t);
			idx+=sizeof(uint8_t);
			if(isseq){
				key_buf[j]=start+i*request_per_command+j;
				idx+=my_itoa(start+i*request_per_command+j, NULL, &buf[idx]);
			}
			else{
				key_buf[j]=start+rand()%(end-start);
				idx+=my_itoa(start+rand()%(end-start), NULL, &buf[idx]);
			}
			(*(uint32_t*)&buf[idx])=0;//offset
			idx+=sizeof(uint32_t);
			m->write_cnt++;
		}

		idx=0;
		m->tbody[i+number_of_command/2].buf=(char*)malloc(request_buf_size + TXNHEADERSIZE);
		buf=m->tbody[i+number_of_command/2].buf;


		idx+=sizeof(uint32_t);//tid
		(*(uint32_t*)&buf[idx])=request_per_command;
		idx+=sizeof(uint32_t);

		for(uint32_t j=0; j<request_per_command; j++){
			(*(uint8_t*)&buf[idx])=FS_GET_T;
			idx+=sizeof(uint8_t);

			(*(uint8_t*)&buf[idx])=KEYLENGTH*16-sizeof(uint32_t);
			idx+=sizeof(uint8_t);
			if(isseq){
				key_buf[j]=start+i*request_per_command+j;
				idx+=my_itoa(start+i*request_per_command+j, NULL, &buf[idx]);
			}
			else{
				key_buf[j]=start+rand()%(end-start);
				idx+=my_itoa(start+rand()%(end-start), NULL, &buf[idx]);
			}
			(*(uint32_t*)&buf[idx])=0;//offset
			idx+=sizeof(uint32_t);
			m->read_cnt++;
		}	
	}

	free(key_buf);
}


void *bench_transaction_end_req(void *_req){
	vec_request *vec=(vec_request*)_req;
	monitor *m=&_master->m[vec->mark];
	
	m->command_return_num++;

	free(vec->req_array);
	free(vec->buf);
	free(vec);
	return NULL;
}
