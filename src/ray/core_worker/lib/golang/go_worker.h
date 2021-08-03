#ifndef _Included_golang_worker
#define _Included_golang_worker
#include <stdbool.h>
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

struct DataBuffer {
  int size;
  uint8_t *p;
};

void go_worker_Initialize(int workerMode, char *store_socket, char *raylet_socket,
                          char *log_dir, char *node_ip_address, int node_manager_port,
                          char *raylet_ip_address, char *driver_name, int jobId,
                          char *redis_address, int redis_port, char *redis_password,
                          char *serialized_job_config);

void go_worker_Run();

void *go_worker_CreateGlobalStateAccessor(char *redis_address, char *redis_password);

bool go_worker_GlobalStateAccessorConnet(void *p);

char *go_worker_GlobalStateAccessorGetInternalKV(void *p, char *key);

int go_worker_GetNodeToConnectForDriver(void *p, char *node_ip_address, char **result);

int go_worker_GetNextJobID(void *p);

int go_worker_CreateActor(char *type_name, char **result);

int go_worker_SubmitActorTask(void *actor_id, char *method_name, char ***return_ids);

extern void go_worker_execute(GoSlice args, GoSlice args)

#ifdef __cplusplus
}
#endif
#endif
