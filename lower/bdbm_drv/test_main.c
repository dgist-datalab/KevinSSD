#include <stdio.h>
#include "../../include/container.h"
#include "bb_checker.h"

char testbit[(REALSIZE/PAGESIZE)/8];
int main(){
	KEYT bad, changed;

	bb_checker_start();
	for(int i=0; i<89;i++){
		scanf("%d%d",&bad,&changed);
		bb_checker_process(bad/_PPS,1);
	}

	printf("bad checking\n");
	
	bb_checker_fixing();
	printf("\n\n RNAGE:%d\n",2*RANGE/8);
	for(KEYT i=0; i<RANGE; i++){
		KEYT res=bb_checker_fix_ppa(i);
		KEYT num=res/8;	
		KEYT offset=res%8;
		if(testbit[num]&(1<<offset)){
			printf("wtf origin:%u changed:%d\n",i,res);
			KEYT res=bb_checker_fix_ppa(i);
			exit(1);
		}
		else{
			testbit[num]|=(1<<offset);
		}
	}
}
