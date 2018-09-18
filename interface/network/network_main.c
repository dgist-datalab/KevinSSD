#include "../../include/container.h"
#include "../bb_checker.h"
#include "../../lower/network/network.h"
#include "../lfqueue.h"
#include "../interface.h"
#include "../../bench/bench.h"
#include <pthread.h>

#define IP "127.0.0.1"
#define PORT 9999


struct serv_params {
    struct net_data *data;
    value_set *vs;
};

#if defined(bdbm_drv)
extern struct lower_info memio_info;
#elif defined(posix_memory)
extern struct lower_info my_posix;
#endif

int serv_fd, clnt_fd;
struct sockaddr_in serv_addr, clnt_addr;
socklen_t clnt_sz;

queue *end_req_q;

pthread_t tid;

void *reactor(void *arg) {
    struct net_data *sent;

    // TODO: How to get end_req here?
    while (1) {
        if (sent = (struct net_data *)q_dequeue(end_req_q)) {
            write(clnt_fd, sent, sizeof(struct net_data));
            free(sent);
        }
    }

    pthread_exit(NULL);
}

void *serv_end_req(algo_req *req) {
    struct serv_params *params = (struct serv_params *)req->params;

    q_enqueue((void *)(params->data), end_req_q);

    if (params->data->type == RQ_TYPE_PUSH) {
        inf_free_valueset(params->vs, FS_MALLOC_W);
    } else if (params->data->type == RQ_TYPE_PULL) {
        inf_free_valueset(params->vs, FS_MALLOC_R);
    }
    free(params);
    free(req);
}

int main(){
    struct lower_info *li;
    struct net_data data;

    int8_t type;
    KEYT ppa;
    algo_req *req;

    algo_req *serv_req;
    struct serv_params *params;
    value_set *dummy_vs;


#if defined(bdbm_drv)
    li = &memio_info;
#elif defined(posix_memory)
    li = &my_posix;
#endif

	li->create(li);
	bb_checker_start(li);

    q_init(&end_req_q, 1024);

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

            serv_req = (algo_req *)malloc(sizeof(algo_req));
            serv_req->end_req = serv_end_req;

            params = (struct serv_params *)malloc(sizeof(struct serv_params));
            params->data = (struct net_data *)malloc(sizeof(struct net_data));
            *(params->data) = data;
            params->vs = dummy_vs;
            serv_req->params = (void *)params;

            li->push_data(ppa, PAGESIZE, dummy_vs, ASYNC, serv_req);
            break;
        case RQ_TYPE_PULL:
            dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);

            serv_req = (algo_req *)malloc(sizeof(algo_req));
            serv_req->end_req = serv_end_req;

            params = (struct serv_params *)malloc(sizeof(struct serv_params));
            params->data = (struct net_data *)malloc(sizeof(struct net_data));
            *(params->data) = data;
            params->vs = dummy_vs;
            serv_req->params = (void *)params;

            li->pull_data(ppa, PAGESIZE, dummy_vs, ASYNC, serv_req);
            break;
        case RQ_TYPE_TRIM:
            li->trim_block(ppa, ASYNC);
            break;
        case RQ_TYPE_FLYING:
            li->lower_flying_req_wait();
            write(clnt_fd, &data, sizeof(data));
            // TODO: unlocking message
            break;
        }
    }

    q_free(end_req_q);
}
