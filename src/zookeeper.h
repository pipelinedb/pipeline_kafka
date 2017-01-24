/*-------------------------------------------------------------------------
 *
 * zookeeper.h
 *
 *	  ZooKeeper interface for pipeline_kafka
 *
 * Copyright (c) 2016, PipelineDB
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_KAFKA_ZK
#define PIPELINE_KAFKA_ZK

#include "zookeeper/zookeeper.h"

typedef struct zk_lock_t
{
	char *name;
	char *lock_znode;
	char *waiting_on_znode;
} zk_lock_t;

extern void init_zk(char *zks, char *zk_root, int session_timeout);
extern zk_lock_t *zk_lock_new(char *name);
extern void acquire_zk_lock(zk_lock_t *lock);
extern void defer_zk_lock(zk_lock_t *lock);
extern bool zk_create(char *path);
extern bool zk_exists(char *path);
extern bool is_zk_lock_held(zk_lock_t *lock);
extern void zk_close(void);

#endif /* PIPELINE_KAFKA_ZK */
