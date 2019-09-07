#include "array.h"
#include "pipe.h"
#include "../../../../include/settings.h"
#include "../../../../bench/bench.h"
#include "../../compaction.h"
#include "../../nocpy.h"
extern MeasureTime write_opt_time[10];
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
void temp_func(char* body, level *d, bool insert){
	int idx;
	uint16_t *bitmap=(uint16_t*)body;
	KEYT key;
	ppa_t *ppa_ptr;
	for_each_header_start(idx,key,ppa_ptr,bitmap,body)
		if(KEYCONSTCOMP(key,"215155000000")==0){
			if(insert)
				printf("insert into %d\n",d->idx);
			else{
				printf("cutter %d\n",d->idx);
			}
		}
	for_each_header_end
}

void array_pipe_merger(struct skiplist* mem, r_pri** s, r_pri** o, struct level* d){
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
//		temp_func(u_data[0],d,true);
	}
	else{
		for(int i=0; s[i]!=NULL; i++) u_num++;
		u_data=(char**)malloc(sizeof(char*)*u_num);
		for(int i=0; i<u_num; i++) {
			u_data[i]=data_from_run(s[i]);
			if(!u_data[i]) abort();
			//temp_func(u_data[i],d,true);
		}
	}

	for(int i=0;o[i]!=NULL ;i++) o_num++;
	char **o_data=(char**)malloc(sizeof(char*)*o_num);
	for(int i=0; o[i]!=NULL; i++){ 
		o_data[i]=data_from_run(o[i]);
		if(!o_data[i]) abort();
		//temp_func(o_data[i],d,true);
	}

	r_data=(char**)calloc(sizeof(char*),(o_num+u_num+LSM.result_padding));
	p_body *lp, *hp;
	lp=pbody_init(o_data,o_num,NULL,false,d->fpr,false);
	hp=pbody_init(u_data,u_num,NULL,false,d->fpr,false);
	rp=pbody_init(r_data,o_num+u_num+LSM.result_padding,NULL,false,d->fpr,true);

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
		/*
		if(KEYCONSTCOMP(insert_key,"215155000000")==0){
			printf("----real insert into %d\n",d->idx);
		}*/
#ifdef BLOOM
		if((pbody_insert_new_key(rp,insert_key,rppa,false,NULL)))
#else
		if((pbody_insert_new_key(rp,insert_key,rppa,false)))
#endif
		{
			result_cnt++;
		}
		
		if(next_pop<0) lp_key=pbody_get_next_key(lp,&lppa);
		else if(next_pop>0) hp_key=pbody_get_next_key(hp,&hppa);
		else{
			lp_key=pbody_get_next_key(lp,&lppa);
			hp_key=pbody_get_next_key(hp,&hppa);
		}
	}
#ifdef BLOOM
	if((pbody_insert_new_key(rp,insert_key,rppa,true,NULL)))
#else
	if((pbody_insert_new_key(rp,insert_key,rppa,true)))
#endif
		{
			result_cnt++;
		}	

	if(mem) free(u_data[0]);
	free(o_data);
	free(u_data);
	pbody_clear(lp);
	pbody_clear(hp);
}
#ifdef BLOOM
run_t *array_pipe_make_run(char *data,uint32_t level_idx, BF *f)
#else
run_t *array_pipe_make_run(char *data,uint32_t level_idx)
#endif
{
	KEYT start,end;
	uint16_t *body=(uint16_t*)data;
	uint32_t num=body[0];

	start.len=body[2]-body[1]-sizeof(ppa_t);
	start.key=&data[body[1]+sizeof(ppa_t)];

	end.len=body[num+1]-body[num]-sizeof(ppa_t);
	end.key=&data[body[num]+sizeof(ppa_t)];
	
	run_t *r=array_make_run(start,end,-1);

#ifdef BLOOM
	r->filter=f;
#endif
	if(level_idx<LSM.LEVELCACHING){
		r->rp->level_caching_data=data;
	}
	else{
		htable *res=LSM.nocpy?htable_assign(data,0):htable_assign(data,1);
		r->rp->cpt_data=res;
		free(data);
	}
	return r;
}

run_t *array_pipe_cutter(struct skiplist* mem, struct level* d, KEYT* _start, KEYT *_end){
	char *data;
#ifdef BLOOM
	BF *f;
#endif
	if(cutter_start){
		cutter_start=false;
#ifdef BLOOM
		data=pbody_get_data(rp,true,&f);
#else
		data=pbody_get_data(rp,true);
#endif
	}
	else{
#ifdef BLOOM
		data=pbody_get_data(rp,false,&f);
#else
		data=pbody_get_data(rp,false);
#endif
	}
	if(!data) {
		free(r_data);
		pbody_clear(rp);
		return NULL;
	}
	
	//temp_func(data,d,false);
#ifdef BLOOM
	return array_pipe_make_run(data,d->idx,f);
#else
	return array_pipe_make_run(data,d->idx);
#endif
}

run_t *array_pipe_p_merger_cutter(skiplist *skip, pl_run *u_data, pl_run* l_data, uint32_t u_num, uint32_t l_num,level *d, void *(*lev_insert_write)(level *,run_t *data)){

	char *skip_data;
	if(skip){
		u_num=1;
		u_data=(pl_run*)malloc(sizeof(pl_run));
		u_data[0].lock=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
		fdriver_lock_init(u_data[0].lock,1);
		skip_data=array_skip_cvt2_data(skip);
		u_data[0].rp=(r_pri*)malloc(sizeof(r_pri));
		u_data[0].rp->level_caching_data=skip_data;
		/*
#ifdef BLOOM
		u_data[0].rp=(array_pipe_make_run(skip_data,-1,NULL);
#else
		u_data[0].rp=array_pipe_make_run(skip_data,-1);
#endif*/
	}

	p_body *lp, *hp, *p_rp;
	char **r_datas=(char**)calloc(sizeof(char*),(u_num+l_num+LSM.result_padding));
	lp=pbody_init(NULL,l_num,l_data,true,d->fpr,false);
	hp=pbody_init(NULL,u_num,u_data,true,d->fpr,false);
	p_rp=pbody_init(r_datas,u_num+l_num+LSM.result_padding,NULL,false,d->fpr,true);
	
	int result_cnt=0;
	int flushed_idx=0;
	run_t **result_temp=(run_t**)malloc(sizeof(run_t*)*(u_num+l_num+LSM.result_padding));

	uint32_t lppa, hppa, p_rppa;
	KEYT lp_key=pbody_get_next_key(lp,&lppa);
	KEYT hp_key=pbody_get_next_key(hp,&hppa);
	KEYT insert_key;
	int next_pop=0;
	char *res_data;
#ifdef BLOOM
	BF *filter;
#endif
	while(!(lp_key.len==UINT8_MAX && hp_key.len==UINT8_MAX)){
		if(lp_key.len==UINT8_MAX){
			insert_key=hp_key;
			p_rppa=hppa;
			next_pop=1;
		}
		else if(hp_key.len==UINT8_MAX){
			insert_key=lp_key;
			p_rppa=lppa;
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
				p_rppa=lppa;
			}
			else if(next_pop>0){
				insert_key=hp_key;
				p_rppa=hppa;
			}
			else{
				invalidate_PPA(DATA,lppa);
				p_rppa=hppa;
				insert_key=hp_key;
			}
		}
#ifdef BLOOM
		if((res_data=pbody_insert_new_key(p_rp,insert_key,p_rppa,false,NULL)))
#else
		if((res_data=pbody_insert_new_key(p_rp,insert_key,p_rppa,false)))
#endif
		{
			/*
#ifdef BOOM
			result_temp[result_cnt++]=array_pipe_make_run(res_data,d->idx,filter);
#else
			result_temp[result_cnt++]=array_pipe_make_run(res_data,d->idx);
#endif	*/
		}

		if(next_pop<0) lp_key=pbody_get_next_key(lp,&lppa);
		else if(next_pop>0) hp_key=pbody_get_next_key(hp,&hppa);
		else{
			lp_key=pbody_get_next_key(lp,&lppa);
			hp_key=pbody_get_next_key(hp,&hppa);
		}
	}

#ifdef BLOOM
	if((res_data=pbody_insert_new_key(p_rp,insert_key,0,true,&filter)))
#else
	if((res_data=pbody_insert_new_key(p_rp,insert_key,0,true)))
#endif
	{
		/*
#ifdef BLOOM
		result_temp[result_cnt++]=array_pipe_make_run(res_data,d->idx,filter);
#else
		result_temp[result_cnt++]=array_pipe_make_run(res_data,d->idx);
#endif	*/

	}


#ifdef BLOOM
	res_data=pbody_get_data(p_rp,true,&filter);
#else
	res_data=pbody_get_data(p_rp,true);
#endif
	do{
		if(!res_data) break;
	//	printf("%d\n",result_cnt);
#ifdef BLOOM
		result_temp[result_cnt++]=array_pipe_make_run(res_data,d->idx,filter);
#else
		result_temp[result_cnt++]=array_pipe_make_run(res_data,d->idx);
#endif		

	}
#ifdef BLOOM
	while((res_data=pbody_get_data(p_rp,false,&filter)));
#else
	while((res_data=pbody_get_data(p_rp,false)));
#endif
	
	for(int i=flushed_idx; i<result_cnt; i++){
		lev_insert_write(d,result_temp[i]);
	}

	if(skip){
		fdriver_destroy(u_data[0].lock);
		free(u_data[0].lock);
		free(u_data[0].rp->level_caching_data);
		free(u_data[0].rp);
		/*
		array_free_run(u_data[0].r);
		htable_free(u_data[0].r->cpt_data);
		free(u_data[0].r);*/
		free(u_data);
	}
	free(result_temp);
	free(r_datas);
	pbody_clear(p_rp);
	pbody_clear(lp);
	pbody_clear(hp);
	return NULL;
}
