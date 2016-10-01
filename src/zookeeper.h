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

typedef struct ZookeeperLock
{
	char *path;
} ZooKeeperLock;

extern void init_zookeeper(char *zks, char *zk_root, char *group, int session_timeout);
extern ZooKeeperLock *acquire_lock(char *path);
extern bool is_lock_held(ZooKeeperLock *lock);
extern void release_lock(ZooKeeperLock *lock);

#endif /* PIPELINE_KAFKA_ZK */
