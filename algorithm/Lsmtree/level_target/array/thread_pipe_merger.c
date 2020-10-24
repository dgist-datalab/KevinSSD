#include "array.h"
#include "pipe.h"
#include "../../../../include/settings.h"
#include "../../../../bench/bench.h"
#include "../../../../include/utils/thpool.h"
#include "../../../../interface/koo_hg_inf.h"
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
extern _bc bc;
bool header_debug_flag;
bool isclear;
static void temp_func(char* body, level *d, bool merger){
	int idx;
	uint16_t *bitmap=(uint16_t*)body;
	KEYT key;
	KEYT prev_key;
	ppa_t *ppa_ptr;
	for_each_header_start(idx,key,ppa_ptr,bitmap,body)
		if(key_const_compare(key, 'd', 29361, 33, NULL)){
			char buf[100];
			key_interpreter(key, buf);			
			printf("maybe update KEY-(%s), ppa:%u ",buf,*ppa_ptr);
			if(key.len==0){
				printf("error!\n");
				abort();
			}
			if(merger)
				printf("insert into %d\n",d->idx);
			else{
				printf("cutter %d\n",d->idx);
			}
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
	bool isdone;
	bool isdummy;
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
	res->isdummy=false;
	res->isdone=false;
	return res;
}

static tp *init_dummy_thread_params(uint32_t num, char *target, level *d, uint32_t idx){
	tp *res=(tp*)malloc(sizeof(tp));
	res->u_data=(char**)malloc(sizeof(char*)*1);
	res->u_data[0]=target;
	res->o_data=NULL;
	res->o_num=0;

	res->result_data=(char**)malloc(sizeof(char*)*num);
	for(uint32_t i=0; i<num; i++){
		res->result_data[i]=(char*)malloc(PAGESIZE);
		memcpy(res->result_data[i], target, PAGESIZE);
	}
	res->rp=pbody_move_dummy_init(res->result_data, num);
	res->result_num=num;

	res->result_return_num=0;
	res->d=d;
	res->idx=idx;
	res->isdummy=true;
	res->isdone=true;
	return res;
}

void tp_print(tp *t){
	if(!t->isdummy){
	printf("[%d] %.*s ~ %.*s(1) && %.*s ~ %.*s (%d)\n",t->idx,
			KEYFORMAT(__extract_start_key(t->u_data[0])),
			KEYFORMAT(__extract_end_key(t->u_data[0])),
			KEYFORMAT(__extract_start_key(t->o_data[0])),
			KEYFORMAT(__extract_end_key(t->o_data[t->o_num-1])),		
			t->o_num
			);
	}
}

void tp_check_sanity(tp **_t, int num){
	KEYT pe,ns;
	for(int i=0; i<num-1; i++){
		if(_t[i]->isdummy || _t[i+1]->isdummy) continue;
		tp *t=_t[i];
		pe=__extract_end_key(t->o_data[t->o_num-1]);
		t=_t[i+1];
		if(*(uint16_t*)t->o_data[0]==0) continue;
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
				invalidate_PPA(DATA,lppa,d->idx);
				rppa=hppa;
				insert_key=hp_key;
			}
		}

		if(d->idx==LSM.LEVELN-1 && !bc.full_caching){
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
	if(d->idx==LSM.LEVELN-1 && !bc.full_caching){
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
	params->isdone=true;
}

thread_params **tpp;
int params_idx;
int params_max;


run_t* array_thread_pipe_cutter(struct skiplist *mem, struct level *d, KEYT *_start, KEYT *_end){
	if(!ismulti_thread) return array_pipe_cutter(mem, d, _start,_end);
	char *data;
retry:
	static int KP_cnt=0;
	thread_params *tp=tpp[params_idx];
	if(!tp->isdone){
		thpool_wait(pool);
	}
	p_body *rp=tp->rp;
	if(cutter_start){
		cutter_start=false;
		data=pbody_get_data(rp, true);
		KP_cnt=0;
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
		if(!isclear){
			bc_clear_ignore_flag();
		}
		return NULL;
	}
	else{
		//temp_func(data, d, false);
	}
	//array_header_print(data);
	//printf("head %d %dprint, %d %d\n", cnt++, rp->pidx,params_max, params_idx);
	//printf("[%d]KP num:%u move:%d\n",KP_cnt++, __get_KP_pair_num(data), tp->isdummy?1:0);
	return array_pipe_make_run(data,d->idx);
}

void array_thread_pipe_merger(struct skiplist* mem, run_t** s, run_t** o, struct level* d){
	if(mem) return array_pipe_merger(mem, s, o, d);
	ismulti_thread=true;
	isclear=false;
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
		//temp_func(u_data[i], d, true);
		if(!u_data[i]) abort();
	}

	if(d->idx==LSM.LEVELN-1 && !bc.full_caching){
		bc_reset();
	}

	for(int i=0;o[i]!=NULL ;i++) o_num++;
	char **o_data=(char**)malloc(sizeof(char*)*o_num);
	for(int i=0; o[i]!=NULL; i++){ 
		o_data[i]=data_from_run(o[i]);
	//	temp_func(o_data[i], d, true);
		if(!o_data[i]) abort();
	}

	tpp=(tp**)malloc(sizeof(tp*)*u_num);
	int tp_num=0, t_data_num;
	int prev_consume_num=0;
	int end_boundary;
	int next_boundary=-1, num;
	bool issplit_start=false;
	char **target_data_set;
	char *splited_data=NULL;
	uint32_t j=0, real_bound;
	KEYT now_end_key, next_start_key;
/*
	printf("upper level!\n");
	array_print(LSM.disk[d->idx-1]);
	printf("lower level!\n");
	array_print(LSM.disk[d->idx]);

	static int cnt=0;
	printf("mg cnt:%d\n", cnt++);
*/
	for(uint32_t i=0; i<u_num; i++){
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

make_params:
		if(end_boundary==-1){
			real_bound=prev_consume_num+end_boundary;
			num=(issplit_start?1:0);
			if(num==0){
				tpp[tp_num]=init_dummy_thread_params(1, u_data[i], d, i);
				goto next_round;
			}
		}
		else{
			real_bound=prev_consume_num+end_boundary;
			num=real_bound-prev_consume_num+(issplit_start?1:0)+1;
		}

		tpp[tp_num]=init_thread_params(num, u_data[i], d, i);
		t_data_num=0;
		target_data_set=tpp[tp_num]->o_data;

		if(issplit_start){
			target_data_set[t_data_num++]=splited_data;
			issplit_start=false;
		}

		if(end_boundary!=-1 && end_boundary==next_boundary){
			splited_data=__split_data(o_data[prev_consume_num+end_boundary], now_end_key, next_start_key, false);
			if(splited_data){
				splited_data_set.push_back(splited_data);
				issplit_start=true;
			}
		}
		else if(end_boundary==-1 && end_boundary==next_boundary){
			splited_data=__split_data(splited_data, now_end_key, next_start_key, false);
			if(splited_data){
				splited_data_set.push_back(splited_data);
				issplit_start=true;				
			}
		}

		if(end_boundary!=-1){
			for(j=prev_consume_num; j<=real_bound; j++){
				target_data_set[t_data_num++]=o_data[j];
				prev_consume_num++;
			}
		}

		tpp[tp_num]->o_num=t_data_num;
		thpool_add_work(pool, __pipe_merger, (void*)tpp[tp_num]);
next_round:
		tp_num++;
	}

	//tp_check_sanity(tpp, tp_num);
	/*
	if(debug){
		for(int i=0; i<tp_num; i++){
			tp_print(tpp[i]);
		}
	}*/


//	exit(1);
	params_max=tp_num;

	//thpool_wait(pool);

	bench_custom_A(write_opt_time2, 9);
	free(o_data);
	free(u_data);
	if(d->idx==LSM.LEVELN-1){
		if(!thpool_num_threads_working(pool)){
			bc_clear_ignore_flag();
			isclear=true;
		}
	}
}

