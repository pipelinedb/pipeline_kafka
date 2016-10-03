/*-------------------------------------------------------------------------
 *
 * zookeeper.c
 *
 *    Zookeeper functionality for pipeline_kafka
 *
 * Copyright (c) 2016, PipelineDB
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <string.h>

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "zookeeper.h"

#define MAX_ZK_LOCKS 32

static zk_lock_t *zk_locks[MAX_ZK_LOCKS];

static char *zk_root = NULL;
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
    int i;

    for (i = 0; i < MAX_ZK_LOCKS; i++)
    {
      if (zk_locks[i] &&
          zk_locks[i]->waiting_on_znode && strcmp(zk_locks[i]->waiting_on_znode, path) == 0)
      {
        zk_locks[i]->waiting_on_znode = NULL;
        break;
      }
    }
  }
  zoo_exists(zk, path, 1, NULL);
}

static char *
get_absolute_zpath(char *lock_name, char *node_path)
{
  StringInfoData buf;

  initStringInfo(&buf);
  appendStringInfo(&buf, "%s/%s", zk_root, lock_name);

  if (node_path)
    appendStringInfo(&buf, "/%s", node_path);

  return buf.data;
}

/*
 * init_zk_lock
 */
void
init_zk(char *zks, char *root, int session_timeout)
{
  struct ACL acl[] = {{ZOO_PERM_CREATE, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_READ, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_DELETE, ZOO_ANYONE_ID_UNSAFE}};
  struct ACL_vector acl_v = {3, acl};
  int rc;
  MemoryContext old;

  zk = zookeeper_init(zks, watcher, session_timeout, 0, 0, 0);
  if (!zk)
    elog(ERROR, "failed to establish zookeeper connection: %m");

  rc = zoo_create(zk, root, NULL, -1, &acl_v, 0, NULL, 0);
  if (rc != ZOK && rc != ZNODEEXISTS)
    elog(ERROR, "failed to create root znode at \"%s\": %m", root);


  old = MemoryContextSwitchTo(CacheMemoryContext);
  zk_root = pstrdup(root);
  MemSet(zk_locks, 0, sizeof(zk_locks));
  MemoryContextSwitchTo(old);
}

zk_lock_t *
zk_lock_new(char *name)
{
  struct ACL acl[] = {{ZOO_PERM_CREATE, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_READ, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_DELETE, ZOO_ANYONE_ID_UNSAFE}};
  struct ACL_vector acl_v = {3, acl};
  StringInfoData buf;
  int rc;
  int i;
  int lock_index = -1;
  MemoryContext old;

  if (!zk_root)
    elog(ERROR, "zookeeper has not been initialized");

  for (i = 0; i < MAX_ZK_LOCKS; i++)
  {
    if (zk_locks[i] == NULL)
    {
      lock_index = i;
      break;
    }
  }

  if (lock_index < 0)
    elog(ERROR, "all %d zk locks have been allocated", MAX_ZK_LOCKS);

  initStringInfo(&buf);
  appendStringInfo(&buf, "%s/%s", zk_root, name);

  rc = zoo_create(zk, buf.data, NULL, -1, &acl_v, 0, NULL, 0);
  if (rc != ZOK && rc != ZNODEEXISTS)
    elog(ERROR, "failed to create group znode at \"%s\": %m", buf.data);

  old = MemoryContextSwitchTo(CacheMemoryContext);
  zk_locks[lock_index] = palloc0(sizeof(zk_lock_t));
  zk_locks[lock_index]->lock_znode = NULL;
  zk_locks[lock_index]->name = pstrdup(name);
  MemoryContextSwitchTo(old);

  pfree(buf.data);

  return zk_locks[lock_index];
}

/*
 * acquire_zk_lock
 *
 * Based on the "Locks" recipe in the ZK docs: https://zookeeper.apache.org/doc/trunk/recipes.html:
 *
 * 1) Create an ephemeral, sequential znode at /prefix/group/lock-.
 * 2) Get children of /prefix/group.
 * 3) If there are no children with a lower sequence number than the node created in 1), the lock is acquired.
 * 4) Add a watch to the child node in front if me, as determined by its sequence number.
 * 5) If the child node from 4) does not exist, goto 2). Otherwise, wait for a delete notification on the node before going to 2).
 */
void
acquire_zk_lock(zk_lock_t *lock)
{
  struct ACL acl[] = {{ZOO_PERM_CREATE, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_READ, ZOO_ANYONE_ID_UNSAFE}, {ZOO_PERM_DELETE, ZOO_ANYONE_ID_UNSAFE}};
  struct ACL_vector acl_v = {3, acl};
  int flags = ZOO_EPHEMERAL | ZOO_SEQUENCE;
  int i;
  struct String_vector children;
  char **nodes;
  StringInfoData created_name;
  char *lock_path;
  char *lock_prefix;
  char *wait_on;
  MemoryContext old;

  if (!zk_root)
    elog(ERROR, "zookeeper has not been initialized");

  lock_path = get_absolute_zpath(lock->name, "lock-");
  lock_prefix = get_absolute_zpath(lock->name, NULL);

  initStringInfo(&created_name);

  /*
   * 1) Create an ephemeral, sequential znode for the given path
   */
  if (zoo_create(zk, lock_path, NULL, -1, &acl_v, flags, created_name.data, created_name.maxlen) != ZOK)
    elog(ERROR, "failed to create lock node for \"%s\": %m", lock_path);

acquire_lock:

  /*
   * 2) Get all lock waiters
   */
  if (zoo_get_children(zk, lock_prefix, 0, &children) != ZOK)
    elog(ERROR, "failed to get children of \"%s\": %m", lock_path);

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
  if (strcmp(created_name.data + strlen(lock_prefix) + 1, nodes[0]) == 0)
    goto lock_acquired;

  /*
   * 4) Add a watch to the child node in front if me, as determined by its sequence number.
   */
  for (i = 1; i < children.count; i++)
  {
    if (strcmp(created_name.data + strlen(lock_prefix) + 1, nodes[i]) == 0)
    {
      wait_on = nodes[i - 1];
      break;
    }
  }

  lock->waiting_on_znode = get_absolute_zpath(lock->name, wait_on);

  /*
   * 5) If the child node from 4) does not exist, goto 2). Otherwise, wait for a delete notification on the node before going to 2).
   */
  if (zoo_exists(zk, lock->waiting_on_znode, 1, NULL) == ZOK)
  {
    wait_on = lock->waiting_on_znode;
    elog(LOG, "waiting on \"%s\" for lock on \"%s\"", lock->waiting_on_znode, lock_prefix);

    for (;;)
    {
      CHECK_FOR_INTERRUPTS();
      if (lock->waiting_on_znode == NULL)
        break;
      pg_usleep(1 * 1000 * 1000);
    }
  }
  goto acquire_lock;

lock_acquired:

  old = MemoryContextSwitchTo(CacheMemoryContext);
  lock->lock_znode = pstrdup(created_name.data);
  MemoryContextSwitchTo(old);

  pfree(lock_path);
  pfree(lock_prefix);
  pfree(created_name.data);

  elog(LOG, "acquired lock on \"%s\"", lock->lock_znode);
}

/*
 * is_zk_lock_held
 */
bool
is_zk_lock_held(zk_lock_t *lock)
{
  struct Stat stat;
  return zoo_exists(zk, lock->lock_znode, 1, &stat) == ZOK;
}
