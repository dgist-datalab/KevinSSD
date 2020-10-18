#include "array.h"
#include "pipe.h"
#include "../../../../include/settings.h"
#include "../../../../bench/bench.h"
#include "../../../../include/utils/thpool.h"
#include "../../compaction.h"
#include "../../nocpy.h"
#include "../../bitmap_cache.h"
#include "mapping_utils.h"
#include <vector>
extern MeasureTime write_opt_time2[10];
std::vector<char*> splited_data_set;
extern lsmtree LSM;
static threadpool pool;
static bool cutter_start;
bool ismulti_thread;
#ifdef BLOOM
extern float t_fpr;
#endif

bool header_debug_flag;
static void temp_func(char* body, level *d, bool merger){
	static int cnt=0;
	int idx;
	uint16_t *bitmap=(uint16_t*)body;
	KEYT key;
	KEYT prev_key;
	ppa_t *ppa_ptr;
	for_each_header_start(idx,key,ppa_ptr,bitmap,body)

		if(KEYCONSTCOMP(key, "00000000000009324279")==0){
			printf("%s %s-%u [level:%u]\n",merger?"merger":"cutter", "00000000000009324279", *ppa_ptr, d->idx);
		}

		if(idx==1){
			prev_key=key;
		}
		else if(idx>1){
			if(KEYCMP(prev_key, key)>=0){
				printf("order is failed!\n");
				abort();
			}
			prev_key=key;
		}
	for_each_header_end
}

typedef struct thread_params{
	uint32_t o_num;
	char **u_data;
	char **o_data;
	uint32_t result_return_num;
	uint32_t result_num;
	char **result_data;
	p_body *rp;
	uint32_t idx;
	struct level *d;
}tp;

static tp *init_thread_params(uint32_t num, char *target, level *d, uint32_t idx){
	tp *res=(tp*)malloc(sizeof(tp));
	res->u_data=(char**)malloc(sizeof(char*)*1);
	res->u_data[0]=target;
	res->o_data=(char**)malloc(sizeof(char*)*num);
	res->o_num=0;
	res->result_return_num=0;
	res->result_num=0;
	res->d=d;
	res->idx=idx;
	return res;
}

void tp_print(tp *t){
	printf("[%d] %.*s ~ %.*s(1) && %.*s ~ %.*s (%d)\n",t->idx,
			KEYFORMAT(__extract_start_key(t->u_data[0])),
			KEYFORMAT(__extract_end_key(t->u_data[0])),
			KEYFORMAT(__extract_start_key(t->o_data[0])),
			KEYFORMAT(__extract_end_key(t->o_data[t->o_num-1])),		
			t->o_num
			);
}

void tp_check_sanity(tp **_t, int num){
	KEYT pe,ns;
	for(int i=0; i<num-1; i++){
		tp *t=_t[i];
		pe=__extract_end_key(t->o_data[t->o_num-1]);
		t=_t[i+1];
		ns=__extract_start_key(t->o_data[0]);
		if(KEYCMP(pe, ns) >=0){
			printf("sanity failed %s:%d\n", __FILE__, __LINE__);
			abort();
		}
	}
}

static void free_thread_params(tp *t){
	free(t->u_data);
	free(t->o_data);
	free(t);
}

void __pipe_merger(void *argument, int id){
	tp *params=(tp*)argument;
	char **o_data=params->o_data;
	char **u_data=params->u_data;
	uint32_t o_num=params->o_num;
	uint32_t u_num=1;
	level *d=params->d;

	static int cnt=0;
	bool debug_temp=false;
	int debug_rp_cnt=0;
	//printf("__pipe_merger :%d\n",cnt++);
	if(cnt++==1333){
		printf("break!!idx:%d\n", params->idx);
		debug_temp=true;
	}

	for(int i=0; i<u_num; i++){
		temp_func(u_data[i], d, 0);
	}
	for(int i=0; i<o_num; i++){
		if(debug_temp){
			printf("%.*s ~ %.*s\n", KEYFORMAT(__extract_start_key(o_data[i])),
					KEYFORMAT(__extract_end_key(o_data[i])));

		}
		temp_func(o_data[i], d, 0);
	}


	char **tp_r_data=(char**)calloc(sizeof(char*),(o_num+u_num+LSM.result_padding));
	p_body *lp, *hp, *rp;
	lp=pbody_init(o_data,o_num,NULL,false,NULL);
	hp=pbody_init(u_data,u_num,NULL,false,NULL);
#ifdef BLOOM
	rp=pbody_init(tp_r_data,o_num+u_num+LSM.result_padding,NULL,false,d->filter);
#else
	rp=pbody_init(tp_r_data,o_num+u_num+LSM.result_padding,NULL,false,NULL);
#endif

	uint32_t lppa, hppa, rppa=0;
	KEYT lp_key=pbody_get_next_key(lp,&lppa);
	KEYT hp_key=pbody_get_next_key(hp,&hppa);
	KEYT insert_key;
	int next_pop=0;
	int result_cnt=0;

	while(!(lp_key.len==UINT8_MAX && hp_key.len==UINT8_MAX)){
		if(lp_key.len==UINT8_MAX){
			insert_key=hp_key;
			rppa=hppa;
			next_pop=1;
		}
		else if(hp_key.len==UINT8_MAX){
			insert_key=lp_key;
			rppa=lppa;
			next_pop=-1;
		}
		else{
			if(!KEYVALCHECK(lp_key)){
				printf("%.*s\n",KEYFORMAT(lp_key));
				abort();
			}
			if(!KEYVALCHECK(hp_key)){
				printf("%.*s\n",KEYFORMAT(hp_key));
				abort();
			}

			next_pop=KEYCMP(lp_key,hp_key);
			if(next_pop<0){
				insert_key=lp_key;
				rppa=lppa;
			}
			else if(next_pop>0){
				insert_key=hp_key;
				rppa=hppa;
			}
			else{
				if(lppa!=UINT32_MAX){
					invalidate_PPA(DATA,lppa);
				}
				rppa=hppa;
				insert_key=hp_key;
			}
		}

		if(debug_temp && KEYCONSTCOMP(insert_key, "00000000000009324279")==0){
			printf("break!\n");
		}

		if(d->idx==LSM.LEVELN-1){
			bc_set_validate(rppa);
		}
	
		if(next_pop<0) lp_key=pbody_get_next_key(lp,&lppa);
		else if(next_pop>0) hp_key=pbody_get_next_key(hp,&hppa);
		else{
			lp_key=pbody_get_next_key(lp,&lppa);
			hp_key=pbody_get_next_key(hp,&hppa);
		}


		if(d->idx==LSM.LEVELN-1 && rppa==TOMBSTONE){
			//printf("ignore key\n");
		}
		else if((pbody_insert_new_key(rp,insert_key,rppa,false))){
			result_cnt++;
		}
	}
	if(d->idx==LSM.LEVELN-1){
		bc_set_validate(rppa);
	}
	
	if((pbody_insert_new_key(rp,insert_key,rppa,true))){
			result_cnt++;
	}
	pbody_clear(lp);
	pbody_clear(hp);
	params->result_num=result_cnt;
	params->result_data=tp_r_data;
	params->rp=rp;
}

thread_params **tpp;
int params_idx;
int params_max;


run_t* array_thread_pipe_cutter(struct skiplist *mem, struct level *d, KEYT *_start, KEYT *_end){
	if(!ismulti_thread) return array_pipe_cutter(mem, d, _start,_end);
	char *data;
retry:
	
	thread_params *tp=tpp[params_idx];
	p_body *rp=tp->rp;
	if(cutter_start){
		cutter_start=false;
		data=pbody_get_data(rp, true);
	}
	else{
		data=pbody_get_data(rp, false);
	}

	if(!data){
		free(tp->result_data);
		pbody_clear(rp);
		if(params_idx < params_max-1){
			params_idx++;
			cutter_start=true;
			goto retry;
		}
		for(int i=0; i<params_max; i++) free_thread_params(tpp[i]);
		for(int i=0; i<splited_data_set.size(); i++){
			free(splited_data_set[i]);
		}
		splited_data_set.clear();
		free(tpp);
		ismulti_thread=false;
		return NULL;
	}
	else{
		temp_func(data, d, false);
	}
	//array_header_print(data);
	//printf("head %d %dprint, %d %d\n", cnt++, rp->pidx,params_max, params_idx);
	return array_pipe_make_run(data,d->idx);
}

void array_thread_pipe_merger(struct skiplist* mem, run_t** s, run_t** o, struct level* d){
	if(mem) return array_pipe_merger(mem, s, o, d);
	ismulti_thread=true;

	static bool is_thread_init=false;
	if(!is_thread_init){
		is_thread_init=true;
#ifdef THREADCOMPACTION
		pool=thpool_init(THREADCOMPACTION);
#endif
	}

	bench_custom_start(write_opt_time2, 9);
	cutter_start=true;
	params_idx=0;
	int o_num=0; int u_num=0;
	char **u_data;
#ifdef BLOOM
	t_fpr=d->fpr;
#endif
	
	for(int i=0; s[i]!=NULL; i++) u_num++;
	u_data=(char**)malloc(sizeof(char*)*u_num);
	for(int i=0; i<u_num; i++) {
		u_data[i]=data_from_run(s[i]);
		temp_func(u_data[i], d, true);
		if(!u_data[i]) abort();
	}

	if(d->idx==LSM.LEVELN-1){
		bc_reset();
	}

	for(int i=0;o[i]!=NULL ;i++) o_num++;
	char **o_data=(char**)malloc(sizeof(char*)*o_num);
	for(int i=0; o[i]!=NULL; i++){ 
		o_data[i]=data_from_run(o[i]);
		temp_func(o_data[i], d, true);
		if(!o_data[i]) abort();
	}


	static int cnt=0;
	//printf("mp_comp cnt:%d\n", cnt++);
	bool debug=false;
	if(cnt++==46){
		printf("break!\n");
		debug=true;
	}

	if(debug){
		printf("%d print\n", d->idx-1);
		array_print(LSM.disk[d->idx-1]);
		printf("%d print\n", d->idx);
		array_print(LSM.disk[d->idx]);
	}

	printf("mp_comp cnt:%d\n", cnt);

	tpp=(tp**)malloc(sizeof(tp*)*o_num);
	int tp_num=0, t_data_num;
	int prev_consume_num=0;
	int end_boundary;
	int next_boundary=-1, num;
	bool issplit_start=false;
	char **target_data_set;
	char *splited_data=NULL;
	uint32_t j=0, real_bound;
	KEYT now_end_key, next_start_key;
	for(uint32_t i=0; i<u_num; i++){
		if(debug){
			printf("i %d\n",i);
		}

		if(debug && i==131){
			printf("ttttt\n");
		}

		if(i==u_num-1){
			end_boundary=o_num-prev_consume_num-1;
			next_boundary=end_boundary+1;
			real_bound=end_boundary+prev_consume_num;
			goto make_params;
		}
		now_end_key=__extract_end_key(u_data[i]);
		next_start_key=__extract_start_key(u_data[i+1]);

		end_boundary=__find_boundary_in_data_list(now_end_key, &o_data[prev_consume_num], o_num-prev_consume_num);
		next_boundary=__find_boundary_in_data_list(next_start_key, &o_data[prev_consume_num], o_num-prev_consume_num);
	
		if(KEYCMP(now_end_key, next_start_key)==0){
			printf("wft!!\n");
			abort();
		}

make_params:
		if(end_boundary==-1){
			real_bound=prev_consume_num+end_boundary;
			num=(issplit_start?1:0);
			if(num==0){
				printf("cant it be?\n");
				abort();
			}
		}
		else{
			real_bound=prev_consume_num+end_boundary;
			num=real_bound-prev_consume_num+(issplit_start?1:0)+1;
		}

	//	printf("%.*s -> %u ~ %u(%u) num:%u splited:%d\n", KEYFORMAT(now_end_key), prev_consume_num, real_bound, prev_consume_num+next_boundary, num, issplit_start);
		tpp[tp_num]=init_thread_params(num, u_data[i], d, i);
		t_data_num=0;
		target_data_set=tpp[tp_num]->o_data;

		if(issplit_start){
			target_data_set[t_data_num++]=splited_data;
			issplit_start=false;

			if(debug && i==131){
				array_header_print(splited_data);
				temp_func(splited_data, d, 0);
			}
		}

		if(end_boundary!=-1 && end_boundary==next_boundary){
			splited_data=__split_data(o_data[prev_consume_num+end_boundary], now_end_key, next_start_key, debug && i==131);
			if(splited_data){
				temp_func(splited_data, d, 0);

				splited_data_set.push_back(splited_data);
				issplit_start=true;
			}
		}
		else if(end_boundary==-1 && end_boundary==next_boundary){
			splited_data=__split_data(splited_data, now_end_key, next_start_key, false);
			if(splited_data){
				temp_func(splited_data, d, 0);

				splited_data_set.push_back(splited_data);
				issplit_start=true;				
			}
		}

		if(end_boundary!=-1){
			for(j=prev_consume_num; j<=real_bound; j++){
				if(debug && i==10){
					temp_func(o_data[j], d, 0);
				}
				target_data_set[t_data_num++]=o_data[j];
				prev_consume_num++;
			}
		}

		tpp[tp_num]->o_num=t_data_num;
		thpool_add_work(pool, __pipe_merger, (void*)tpp[tp_num]);
		tp_num++;
	//	printf("\n");
	}

/*
	for(int i=0; i<tp_num; i++){
		tp_print(tpp[i]);
	}
*/
	tp_check_sanity(tpp, tp_num);

//	exit(1);
	params_max=tp_num;

	thpool_wait(pool);

	bench_custom_A(write_opt_time2, 9);
	free(o_data);
	free(u_data);
}

