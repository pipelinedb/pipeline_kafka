/*-------------------------------------------------------------------------
 *
 * zookeeper.c
 *
 *	  Zookeeper functionality for pipeline_kafka
 *
 * Copyright (c) 2016, PipelineDB
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <string.h>

#include "lib/stringinfo.h"

#include "zookeeper.h"

static char *wait_on = NULL;
static char *zk_prefix = NULL;
static zhandle_t *zk = NULL;

static int
znode_cmp(const void *l, const void *r)
{
	const char *ll = *(const char **) l;
	const char *rr = *(const char **) r;

	return strcmp(ll, rr);
}

static void
watcher(zhandle_t *zk, int type, int state, const char *path, void *context)
{
	if (type == ZOO_DELETED_EVENT)
	{
		if (wait_on && strcmp(wait_on, path) == 0)
			wait_on = NULL;
	}
	zoo_exists(zk, path, 1, NULL);
}

/*
 * init_zookeeper
 */
void
init_zookeeper(char *zks, char *zk_root, char *group, int session_timeout)
{
  struct ACL acl[] = {{ZOO_PERM_CREATE, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_READ, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_DELETE, ZOO_ANYONE_ID_UNSAFE}};
  struct ACL_vector acl_v = {3, acl};
	zhandle_t *zk = zookeeper_init(zks, watcher, session_timeout, 0, 0, 0);
	StringInfoData buf;

	if (!zk)
		elog(ERROR, "failed to establish zookeeper connection: %m");

  if (zoo_create(zk, zk_root, NULL, -1, &acl_v, 0, NULL, 0) != ZOK)
  	elog(ERROR, "failed to create root znode at \"%s\": %m", zk_root);

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s/%s", zk_root, group);
	zk_prefix = buf.data;

  if (zoo_create(zk, buf.data, NULL, -1, &acl_v, 0, NULL, 0) != ZOK)
  	elog(ERROR, "failed to create group znode at \"%s\": %m", buf.data);

  pfree(buf.data);
}

static char *
get_absolute_zpath(char *node_path)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s/%s", zk_prefix, node_path);

	return buf.data;
}

/*
 * acquire_lock
 *
 * Based on the "Locks" recipe in the ZK docs: https://zookeeper.apache.org/doc/trunk/recipes.html:
 *
 * 1) Create an ephemeral, sequential znode at /prefix/group/lock-.
 * 2) Get children of /prefix/group.
 * 3) If there are no children with a lower sequence number than the node created in 1), the lock is acquired.
 * 4) Add a watch to the child node in front if me, as determined by its sequence number.
 * 5) If the child node from 4) does not exist, goto 2). Otherwise, wait for a delete notification on the node before going to 2).
 */
ZooKeeperLock *
acquire_lock(char *path)
{
  struct ACL acl[] = {{ZOO_PERM_CREATE, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_READ, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_DELETE, ZOO_ANYONE_ID_UNSAFE}};
  struct ACL_vector acl_v = {3, acl};
  int flags = ZOO_EPHEMERAL | ZOO_SEQUENCE;
	int i;
  struct String_vector children;
  char **nodes;
  char *next_node;
  StringInfoData created_name;

  initStringInfo(&created_name);

	/*
	 * 1) Create an ephemeral, sequential znode for the given path
	 */
  if (zoo_create(zk, get_absolute_zpath("consumer-"), NULL, -1, &acl_v, flags, created_name.data, created_name.maxlen) != ZOK)
  	elog(ERROR, "failed to create lock node for \"%s\": %m", path);

acquire_lock:

	/*
	 * 2) Get all lock waiters
	 */
	if (zoo_get_children(zk, zk_prefix, 0, &children) != ZOK)
		elog(ERROR, "failed to get children of \"%s\": %m", zk_prefix);

	if (children.count == 1)
	{
		/*
		 * Fast path, I'm the only one who has tried to acquire it
		 */
		goto lock_acquired;
	}

	nodes = palloc0(sizeof(char *) * children.count);
	for (i = 0; i < children.count; i++)
		nodes[i] = children.data[i];

	qsort(nodes, children.count, sizeof(char *), znode_cmp);

	/*
	 * 3) If I'm the first node in the lock queue, I now have the lock
	 */
	if (strcmp(created_name.data + strlen(zk_prefix) + 1, nodes[0]) == 0)
		goto lock_acquired;

	/*
	 * 4) Add a watch to the child node in front if me, as determined by its sequence number.
	 */
	for (i = 1; i < children.count; i++)
	{
		if (strcmp(created_name.data + strlen(zk_prefix) + 1, nodes[i]) == 0)
		{
			wait_on = nodes[i - 1];
			break;
		}
	}

	next_node = get_absolute_zpath(wait_on);

	/*
	 * 5) If the child node from 4) does not exist, goto 2). Otherwise, wait for a delete notification on the node before going to 2).
	 */
	if (zoo_exists(zk, next_node, 1, NULL) == ZOK)
	{
		wait_on = next_node;
		while (wait_on)
			pg_usleep(1 * 1000 * 1000);
	}
	goto acquire_lock;

lock_acquired:

	return NULL;
}

/*
 * is_lock_held
 */
bool
is_lock_held(ZooKeeperLock *lock)
{
	struct Stat stat;
	return zoo_exists(zk, lock->path, 1, &stat) == ZOK;
}

/*
 * release_lock
 */
void
release_lock(ZooKeeperLock *lock)
{

}
