#include "array.h"
#include "pipe.h"
#include "../../../../include/settings.h"
p_body *rp;
char **r_data;
bool cutter_start;
#ifdef BLOOM
float t_fpr;
#endif
extern lsmtree LSM;
char *array_skip_cvt2_data(skiplist *mem){
	char *res=(char*)malloc(PAGESIZE);
	uint16_t *bitmap=(uint16_t *)res;
	uint32_t idx=1;
	uint16_t data_start=KEYBITMAP;
	snode *temp;
	for_each_sk(temp,mem){
		memcpy(&res[data_start],&temp->ppa,sizeof(temp->ppa));
		memcpy(&res[data_start+sizeof(temp->ppa)],temp->key.key,temp->key.len);
		bitmap[idx]=data_start;

		data_start+=temp->key.len+sizeof(temp->ppa);
		idx++;
	}
	bitmap[0]=idx-1;
	bitmap[idx]=data_start;
	return res;
}
void array_pipe_merger(struct skiplist* mem, run_t** s, run_t** o, struct level* d){
	cutter_start=true;
	int o_num=0; int u_num=0;
	char **u_data;
#ifdef BLOOM
	t_fpr=d->fpr;
#endif
	if(mem){
		u_num=1;
		u_data=(char**)malloc(sizeof(char*)*u_num);
		u_data[0]=array_skip_cvt2_data(mem);
	}
	else{
		for(int i=0; s[i]!=NULL; i++) u_num++;
		u_data=(char**)malloc(sizeof(char*)*u_num);
		for(int i=0; i<u_num; i++) {u_data[i]=data_from_run(s[i]);}
	}

	for(int i=0;o[i]!=NULL ;i++) o_num++;
	char **o_data=(char**)malloc(sizeof(char*)*o_num);
	for(int i=0; o[i]!=NULL; i++){ o_data[i]=data_from_run(o[i]);}

	r_data=(char**)calloc(sizeof(char*),(o_num+u_num));
	p_body *lp, *hp;
	lp=pbody_init(o_data,o_num);
	hp=pbody_init(u_data,u_num);
	rp=pbody_init(r_data,o_num+u_num);

	uint32_t lppa, hppa, rppa;
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
				invalidate_PPA(DATA,lppa);
				rppa=hppa;
				insert_key=hp_key;
			}
		}
	
		if((pbody_insert_new_key(rp,insert_key,rppa,false))){
			result_cnt++;
		}
		
		if(next_pop<0) lp_key=pbody_get_next_key(lp,&lppa);
		else if(next_pop>0) hp_key=pbody_get_next_key(hp,&hppa);
		else{
			lp_key=pbody_get_next_key(lp,&lppa);
			hp_key=pbody_get_next_key(hp,&hppa);
		}
	}

	if((pbody_insert_new_key(rp,insert_key,0,true))){
			result_cnt++;
	}

	if(mem) free(u_data[0]);
	free(o_data);
	free(u_data);
	free(lp);
	free(hp);
}

run_t *array_pipe_make_run(char *data){
	htable *res=LSM.nocpy?htable_assign(data,0):htable_assign(data,1);

	KEYT start,end;
	uint16_t *body=(uint16_t*)data;
	uint32_t num=body[0];

	start.len=body[2]-body[1]-sizeof(ppa_t);
	start.key=&data[body[1]+sizeof(ppa_t)];

	end.len=body[num+1]-body[num]-sizeof(ppa_t);
	end.key=&data[body[num]+sizeof(ppa_t)];
	
	run_t *r=array_make_run(start,end,-1);
	r->cpt_data=res;
	free(data);
#ifdef BLOOM
	r->filter=array_making_filter(r,num,t_fpr);
#endif
	return r;
}
run_t *array_pipe_cutter(struct skiplist* mem, struct level* d, KEYT* _start, KEYT *_end){
	char *data;
	if(cutter_start){
		cutter_start=false;
		data=pbody_get_data(rp,true);
	}
	else{
		data=pbody_get_data(rp,false);
	}
	if(!data) {
		free(r_data);
		free(rp);
		return NULL;
	}

	return array_pipe_make_run(data);
}

p_body *pm_lp, *pm_hp;
char *skip_data;
run_t *array_pipe_p_merger_cutter(skiplist *skip,run_t **src, run_t **org, float f, uint32_t unum, uint32_t lnum){
	return NULL;
}
