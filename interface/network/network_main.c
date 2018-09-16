#include "../../include/container.h"
#include "../bb_checker.h"
#include "../../lower/network/network.h"

#define IP "127.0.0.1"
#define PORT 9999

#ifdef bdbm_drv
extern struct lower_info memio_info;
#endif
#ifdef posix_memory
extern struct lower_info my_posix;
#endif

int serv_fd, clnt_fd;
struct sockaddr_in serv_addr, clnt_addr;
socklen_t clnt_sz;

void *reactor(void *arg) {
    struct net_data data;

    // TODO: How to get end_req here?
    while (1) {

    }

    pthread_exit(1);
}

int main(){
    struct lower_info *li;
    struct net_data data;

    int8_t type;
    KEYT ppa;
    algo_req *req;

#ifdef bdbm_drv
    li = &memio_info;
#endif
#ifdef posix_memory
    li = &my_posix;
#endif

	li->create(li);
	bb_checker_start(li);

    serv_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (serv_fd == -1) {
        perror("Socket openning ERROR");
        exit(1);
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(PORT);

    if (bind(serv_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) == -1) {
        perror("Binding ERROR");
        exit(1);
    }
    if (listen(serv_fd, 5) == -1) {
        perror("Listening ERROR");
        exit(1);
    }

    clnt_sz = sizeof(clnt_addr);
    clnt_fd = accept(serv_fd, (struct sockaddr *)&clnt_addr, &clnt_sz);
    if (clnt_sz == -1) {
        perror("Accepting ERROR");
        exit(1);
    }

    pthread_create(&tid, NULL, reactor, NULL);

    while (read(clnt_fd, &data, sizeof(data))) {
        type = ((struct net_data *)&data)->type;
        ppa  = ((struct net_data *)&data)->ppa;
        req  = ((struct net_data *)&data)->req;

        switch (type) {
        case RQ_TYPE_DESTROY:
            li->destroy(li);
            break;
        case RQ_TYPE_PUSH:
            dummy_vs = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
            // TODO: end_req
            li->push_data(ppa, PAGESIZE, dummy_vs, ASYNC, req);
            break;
        case RQ_TYPE_PULL:
            dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
            // TODO: end_req
            li->pull_data(ppa, PAGESIZE, dummy_vs, ASYNC, req);
            break;
        case RQ_TYPE_TRIM:
            li->trim_block(ppa, ASYNC);
            break;
        case RQ_TYPE_FLYING:
            li->flyling_req_wait();
            // TODO: unlocking message
            break;
        }
    }
}
