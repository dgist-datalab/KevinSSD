#include "../../include/container.h"
#include "../bb_checker.h"
#ifdef bdbm_drv
extern struct lower_info memio_info;
#endif

int main(){
	memio_info.create(&memio_info);
	bb_checker_start(&memio_info);
}
