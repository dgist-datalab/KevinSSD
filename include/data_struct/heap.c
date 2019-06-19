#include "heap.h"
#include <stdio.h>
#include <stdlib.h>


void mh_init(mh** h, int bn, void(*a)(void*,void*), void(*b)(void*a, void*)){
	*h=(mh*)malloc(sizeof(mh));
	(*h)->size=0;
	(*h)->max=bn;

	(*h)->body=(hn*)calloc(sizeof(hn),(bn+1));
	(*h)->swap_hptr=a;
	(*h)->assign_hptr=b;
}

void mh_free(mh* h){
	free(h->body);
	free(h);
}
static hn* minchild(mh *h, hn *n){
	hn *res=NULL;
	int idx=MHGETIDX(h,n);

	hn *lc=MHL_CHIPTR(h,idx);
	hn *rc=MHR_CHIPTR(h,idx);

	if(lc->data && !rc->data) res=lc;
	else if(!lc->data && rc->data) res=rc;
	else if(lc->data && rc->data) res=lc->cnt<rc->cnt?lc:rc;

	return res;
}

hn* mh_internal_update(mh *h, hn* n){
	int idx=MHGETIDX(h,n);
	hn *chgd=n;
	while(idx>1){
		hn *p=MHPARENTPTR(h,idx);
		if(p->cnt<n->cnt){
			int temp=n->cnt;
			n->cnt=p->cnt;
			p->cnt=temp;

			h->swap_hptr(p->data,n->data);
			chgd=p;
		}
		else break;
		idx/=2;
	}
	return chgd;
}

hn* mh_internal_downdate(mh *h, hn *n){
	hn *chgd=n;
	while(1){
		hn *child=minchild(h,chgd);
		if(child){
			if(child->cnt > chgd->cnt){
				int temp=child->cnt;
				child->cnt=chgd->cnt;
				chgd->cnt=temp;

				h->swap_hptr(child->data,chgd->data);
				chgd=child;
			}
			else break;
		}
		else break;
	}
	return chgd;
}

void mh_insert(mh* h, void *data, int number){
	if(h->size>h->max){
		printf("full heap!\n");
		abort();
		return;
	}
	h->size++;
	hn* n=&h->body[h->size];
	n->data=data;
	n->cnt=number;
	
	h->assign_hptr(data,(void*)n);
	mh_internal_update(h,n);
}

void *mh_get_max(mh* h){
	void *res=(void*)&h->body[1].data;
	h->body[1].data=h->body[h->size].data;
	h->body[1].cnt=h->body[h->size].cnt;

	mh_internal_downdate(h,&h->body[1]);
	h->size-=1;
	return res;
}

void mh_update(mh* h,int number, void *hptr){
	hn* p=(hn*)hptr;
	int temp=p->cnt;
	p->cnt=number;

	if(temp<number)
		mh_internal_update(h,p);
	else
		mh_internal_downdate(h,p);
}
