/*-------------------------------------------------------------------------
 *
 * pipeline_kafka.c
 *
 *	  PipelineDB support for Kafka
 *
 * Copyright (c) 2016, PipelineDB
 *
 * contrib/pipeline_kafka.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>

#include "postgres.h"

#include "funcapi.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_stream_fn.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "lib/stringinfo.h"
#include "librdkafka/rdkafka.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h"
#include "pipeline_kafka.h"
#include "pipeline/stream.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

#define RETURN_SUCCESS() PG_RETURN_DATUM(CStringGetTextDatum("success"))
#define RETURN_FAILURE() PG_RETURN_DATUM(CStringGetTextDatum("failure"))

#define KAFKA_CONSUME_MAIN "kafka_consume_main"
#define PIPELINE_KAFKA_LIB "pipeline_kafka"
#define PIPELINE_KAFKA_SCHEMA "pipeline_kafka"

#define CONSUMER_RELATION "consumers"
#define CONSUMER_RELATION_NATTS		12
#define CONSUMER_ATTR_ID	 		1
#define CONSUMER_ATTR_TOPIC			2
#define CONSUMER_ATTR_RELATION 		3
#define CONSUMER_ATTR_GROUP_ID	4
#define CONSUMER_ATTR_FORMAT 		5
#define CONSUMER_ATTR_DELIMITER 	6
#define CONSUMER_ATTR_QUOTE			7
#define CONSUMER_ATTR_ESCAPE		8
#define CONSUMER_ATTR_BATCH_SIZE 	9
#define CONSUMER_ATTR_MAX_BYTES 	10
#define CONSUMER_ATTR_PARALLELISM 	11
#define CONSUMER_ATTR_TIMEOUT	 	12

#define OFFSETS_RELATION "offsets"
#define OFFSETS_RELATION_NATTS	3
#define OFFSETS_ATTR_CONSUMER 	1
#define OFFSETS_ATTR_PARTITION	2
#define OFFSETS_ATTR_OFFSET 	3

#define BROKER_RELATION "brokers"
#define BROKER_RELATION_NATTS	1
#define BROKER_ATTR_HOST 		1

#define NUM_CONSUMERS_INIT 4
#define NUM_CONSUMERS_MAX 64

#define DEFAULT_PARALLELISM 1
#define MAX_CONSUMER_PROCS 32

#define KAFKA_META_TIMEOUT 1000 /* 1s */

#define OPTION_DELIMITER "delimiter"
#define OPTION_FORMAT "format"
#define FORMAT_CSV "csv"
#define FORMAT_JSON "json"
#define OPTION_QUOTE "quote"
#define OPTION_ESCAPE "escape"

#define FORMAT_JSON_QUOTE "\x01"
#define FORMAT_JSON_DELIMITER "\x02"

#define RD_KAFKA_OFFSET_NULL INT64_MIN

#define CONSUMER_LOG_PREFIX "[pipeline_kafka] %s <- %s (PID %d): "
#define CONSUMER_LOG_PREFIX_PARAMS(consumer) \
	((consumer)->rel ? (consumer)->rel->relname : "*"), (consumer)->topic_name, MyProcPid
#define CONSUMER_WORKER_RESTART_TIME 1

static volatile sig_atomic_t got_SIGTERM = false;
static rd_kafka_t *MyKafka = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static char *broker_version = NULL;
static char *consumer_config = NULL;

void _PG_init(void);

typedef struct error_buf_t
{
	Size             size;
	pg_atomic_uint32 offset;
	char            *bytes;
	slock_t          mutex;
} error_buf_t;

#define ERROR_BUF_SIZE 4096
static error_buf_t my_error_buf;

static void
error_buf_init(error_buf_t *ebuf, Size size)
{
	MemSet(ebuf, 0, sizeof(error_buf_t));

	ebuf->size = size;
	ebuf->bytes = palloc0(size);

	pg_atomic_init_u32(&ebuf->offset, 0);
	SpinLockInit(&ebuf->mutex);
}

static bool
error_buf_push(error_buf_t *ebuf, const char *str)
{
	bool success = false;
	int len = strlen(str) + 1; /* for \n */
	uint32 offset;

	SpinLockAcquire(&ebuf->mutex);

	offset = pg_atomic_read_u32(&ebuf->offset);

	if (ebuf->size - offset > len)
	{
		char *pos = &ebuf->bytes[offset];
		memcpy(pos, str, len);
		offset += len;
		Assert(offset <= ebuf->size);
		pg_atomic_write_u32(&ebuf->offset, offset);
		success = true;
	}

	SpinLockRelease(&ebuf->mutex);

	return success;
}

static char *
error_buf_pop(error_buf_t *ebuf)
{
	char *err;
	uint32 offset = pg_atomic_read_u32(&ebuf->offset);

	if (!offset)
		return NULL;

	SpinLockAcquire(&ebuf->mutex);

	offset = pg_atomic_read_u32(&ebuf->offset);
	err = palloc(offset);
	memcpy(err, ebuf->bytes, offset);
	pg_atomic_write_u32(&ebuf->offset, 0);

	SpinLockRelease(&ebuf->mutex);

	return err;
}

/*
 * Shared-memory state for each consumer process
 */
typedef struct KafkaConsumerProc
{
	int32 id;
	Oid db;
	int32 consumer_id;
	int64 start_offset;
	int partition_group;
	BackgroundWorkerHandle worker;
} KafkaConsumerProc;

typedef struct KafkaConsumerGroupKey
{
	Oid db;
	int32 consumer_id;
} KafkaConsumerGroupKey;

/*
 * Shared-memory state for each consumer process group
 */
typedef struct KafkaConsumerGroup
{
	KafkaConsumerGroupKey key;
	int parallelism;
} KafkaConsumerGroup;

/*
 * Local-memory configuration for a consumer
 */
typedef struct KafkaConsumer
{
	int32 id;
	List *brokers;
	char *topic_name;
	RangeVar *rel;
	int32_t partition;
	int64_t offset;
	int batch_size;
	int max_bytes;
	int parallelism;
	int timeout;
	char *group_id;
	char *format;
	char *delimiter;
	char *quote;
	char *escape;
	int num_partitions;
	int64_t *offsets;
	rd_kafka_t *kafka;
	rd_kafka_topic_t *topic;
} KafkaConsumer;

/* Shared-memory hashtable for storing consumer process group information */
static HTAB *consumer_groups = NULL;

/* Shared-memory hashtable storing all individual consumer process information */
static HTAB *consumer_procs = NULL;

static void
pipeline_kafka_shmem_startup(void)
{
	HASHCTL ctl;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(int32);
	ctl.entrysize = sizeof(KafkaConsumerProc);

	consumer_procs = ShmemInitHash("KafkaConsumerProcs", NUM_CONSUMERS_INIT,
			NUM_CONSUMERS_MAX, &ctl, HASH_ELEM | HASH_BLOBS);

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(KafkaConsumerGroupKey);
	ctl.entrysize = sizeof(KafkaConsumerGroup);

	consumer_groups = ShmemInitHash("KafkaConsumerGroups", 2 * NUM_CONSUMERS_INIT,
			2 * NUM_CONSUMERS_MAX, &ctl, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

static Size
pipeline_kafka_shmem_size(void)
{
	Size size;

	size = hash_estimate_size(max_worker_processes, sizeof(KafkaConsumerGroup));
	size = add_size(size, hash_estimate_size(max_worker_processes, sizeof(KafkaConsumerProc)));

	return size;
}

static void
check_pipeline_kafka_preloaded(void)
{
	if (!consumer_groups)
	{
		Assert(!consumer_procs);
		ereport(ERROR,
				(errmsg("%s wasn't loaded as a shared library", PIPELINE_KAFKA_LIB),
				errhint("Add %s to the shared_preload_libraries configuration parameter.", PIPELINE_KAFKA_LIB)));
	}
}

/*
 * Initialization performed at module-load time
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(WARNING, "%s must be loaded via shared_preload_libraries", PIPELINE_KAFKA_LIB);
		return;
	}

	DefineCustomStringVariable("pipeline_kafka.broker_version",
			gettext_noop("Specifies the Kafka broker version for cases in which it can't be detected."),
			NULL,
			&broker_version,
			NULL,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	DefineCustomStringVariable("pipeline_kafka.consumer_config",
			gettext_noop("Comma-separated list of key-value pairs to override the default librdkafka configuration with."),
			NULL,
			&consumer_config,
			NULL,
			PGC_POSTMASTER, 0,
			NULL, NULL, NULL);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pipeline_kafka_shmem_startup;

	RequestAddinShmemSpace(MAXALIGN(pipeline_kafka_shmem_size()));
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
kafka_consume_main_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_SIGTERM = true;
	if (MyLatch)
		SetLatch(MyLatch);

	errno = save_errno;
}

static RangeVar *
get_rangevar(char *relname)
{
	return makeRangeVar(PIPELINE_KAFKA_SCHEMA, relname, -1);
}

static ResultRelInfo *
relinfo_open(RangeVar *rv, LOCKMODE mode)
{
	Relation rel = relation_openrv(rv, mode);
	ResultRelInfo *rinfo = makeNode(ResultRelInfo);

	rinfo->ri_RangeTableIndex = 1; /* dummy */
	rinfo->ri_RelationDesc = rel;
	rinfo->ri_TrigDesc = NULL;

	ExecOpenIndices(rinfo, false);

	return rinfo;
}

static void
relinfo_close(ResultRelInfo *rinfo, LOCKMODE mode)
{
	ExecCloseIndices(rinfo);
	relation_close(rinfo->ri_RelationDesc, mode);
	pfree(rinfo);
}

static void
update_indices(ResultRelInfo *rinfo, HeapTuple tup)
{
	EState *estate = CreateExecutorState();
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rinfo->ri_RelationDesc));
	ExecStoreTuple(tup, slot, InvalidBuffer, false);
	estate->es_result_relation_info = rinfo;
	ExecInsertIndexTuples(slot, &tup->t_self, estate, false, NULL, NIL);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
}

static void
relinfo_insert(ResultRelInfo *rinfo, HeapTuple tup)
{
	simple_heap_insert(rinfo->ri_RelationDesc, tup);
	update_indices(rinfo, tup);
}

static void
relinfo_update(ResultRelInfo *rinfo, ItemPointer tid, HeapTuple tup)
{
	simple_heap_update(rinfo->ri_RelationDesc, tid, tup);
	update_indices(rinfo, tup);
}

static void
relinfo_delete(ResultRelInfo *rinfo, ItemPointer tid)
{
	simple_heap_delete(rinfo->ri_RelationDesc, tid);
}

/*
 * librdkafka consumer logger function
 */
static void
consumer_logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
	error_buf_push(&my_error_buf, buf);
}

/*
 * get_all_brokers
 *
 * Return a list of all brokers in pipeline_kafka_brokers
 */
static List *
get_all_brokers(void)
{
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	ResultRelInfo *brokers = relinfo_open(get_rangevar(BROKER_RELATION), AccessShareLock);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(brokers->ri_RelationDesc));
	List *result = NIL;

	scan = heap_beginscan(brokers->ri_RelationDesc, GetTransactionSnapshot(), 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		char *host;
		Datum d;
		bool isnull;

		ExecStoreTuple(tup, slot, InvalidBuffer, false);
		d = slot_getattr(slot, BROKER_ATTR_HOST, &isnull);
		host = TextDatumGetCString(d);

		result = lappend(result, host);
	}

	ExecDropSingleTupleTableSlot(slot);
	heap_endscan(scan);
	relinfo_close(brokers, NoLock);

	return result;
}

/*
 * load_consumer_offsets
 *
 * Load all offsets for all of this consumer's partitions
 */
static void
load_consumer_offsets(KafkaConsumer *consumer, struct rd_kafka_metadata_topic *meta, int64_t start_offset)
{
	MemoryContext old;
	ScanKeyData skey[1];
	HeapTuple tup = NULL;
	IndexScanDesc scan;
	ResultRelInfo *offsets;
	TupleTableSlot *slot;
	int i;

	old = MemoryContextSwitchTo(CacheMemoryContext);
	consumer->offsets = palloc0(meta->partition_cnt * sizeof(int64_t));
	MemoryContextSwitchTo(old);

	/* by default, begin consuming from the end of a stream */
	for (i = 0; i < meta->partition_cnt; i++)
		consumer->offsets[i] = start_offset;

	/*
	 * Consumers with a group get their offsets from brokers
	 */
	if (consumer->group_id)
		return;

	offsets = relinfo_open(get_rangevar(OFFSETS_RELATION), RowExclusiveLock);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(offsets->ri_RelationDesc));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(consumer->id));
	scan = index_beginscan(offsets->ri_RelationDesc, offsets->ri_IndexRelationDescs[1],
			GetTransactionSnapshot(), 1, 0);
	index_rescan(scan, skey, 1, NULL, 0);

	while ((tup = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum d;
		bool isnull;
		int partition;
		int64_t offset;

		ExecStoreTuple(tup, slot, InvalidBuffer, false);

		d = slot_getattr(slot, OFFSETS_ATTR_PARTITION, &isnull);
		partition = DatumGetInt32(d);

		if (partition > consumer->num_partitions)
			elog(ERROR, "invalid partition id: %d", partition);

		if (start_offset == RD_KAFKA_OFFSET_NULL)
		{
			d = slot_getattr(slot, OFFSETS_ATTR_OFFSET, &isnull);
			if (isnull)
				offset = RD_KAFKA_OFFSET_END;
			else
			{
				/*
				 * Add one so that we start consuming from the message after the one we consumed
				 * most recently.
				 */
				offset = DatumGetInt64(d) + 1;
			}
		}
		else
			offset = start_offset;

		consumer->offsets[partition] = DatumGetInt64(offset);
	}

	/* If no offset was saved and we passed it a NULL start_offset, set it to END */
	for (i = 0; i < meta->partition_cnt; i++)
	{
		if (consumer->offsets[i] == RD_KAFKA_OFFSET_NULL)
			consumer->offsets[i] = RD_KAFKA_OFFSET_END;
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	relinfo_close(offsets, NoLock);
}

/*
 * load_consumer_state
 *
 * Read consumer state from pipeline_kafka_consumers into the given struct
 */
static void
load_consumer_state(int32 consumer_id, KafkaConsumer *consumer)
{
	ScanKeyData skey[1];
	HeapTuple tup = NULL;
	IndexScanDesc scan;
	ResultRelInfo *consumers = relinfo_open(get_rangevar(CONSUMER_RELATION), ExclusiveLock);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(consumers->ri_RelationDesc));
	Datum d;
	bool isnull;
	text *qualified;
	MemoryContext old;
	char *relname;

	MemSet(consumer, 0, sizeof(KafkaConsumer));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(consumer_id));
	scan = index_beginscan(consumers->ri_RelationDesc, consumers->ri_IndexRelationDescs[0],
			GetTransactionSnapshot(), 1, 0);
	index_rescan(scan, skey, 1, NULL, 0);
	tup = index_getnext(scan, ForwardScanDirection);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "kafka consumer %d not found", consumer_id);

	ExecStoreTuple(tup, slot, InvalidBuffer, false);

	/* we don't want anything that's palloc'd to get freed when we commit */
	old = MemoryContextSwitchTo(CacheMemoryContext);

	d = slot_getattr(slot, CONSUMER_ATTR_ID, &isnull);
	Assert(!isnull);
	consumer->id = DatumGetInt32(d);

	/* target relation */
	d = slot_getattr(slot, CONSUMER_ATTR_RELATION, &isnull);
	Assert(!isnull);
	qualified = (text *) DatumGetPointer(d);
	relname = text_to_cstring(qualified);
	if (strlen(relname))
		consumer->rel = makeRangeVarFromNameList(textToQualifiedNameList(qualified));
	else
		consumer->rel = NULL;

	/* topic */
	d = slot_getattr(slot, CONSUMER_ATTR_TOPIC, &isnull);
	Assert(!isnull);
	consumer->topic_name = TextDatumGetCString(d);

	/* consumer group id */
	d = slot_getattr(slot, CONSUMER_ATTR_GROUP_ID, &isnull);
	if (!isnull)
		consumer->group_id = TextDatumGetCString(d);
	else
		consumer->group_id = NULL;

	/* format */
	d = slot_getattr(slot, CONSUMER_ATTR_FORMAT, &isnull);
	consumer->format = TextDatumGetCString(d);

	/* delimiter */
	d = slot_getattr(slot, CONSUMER_ATTR_DELIMITER, &isnull);
	if (!isnull)
		consumer->delimiter = TextDatumGetCString(d);
	else
		consumer->delimiter = NULL;

	/* quote character */
	d = slot_getattr(slot, CONSUMER_ATTR_QUOTE, &isnull);
	if (!isnull)
		consumer->quote = TextDatumGetCString(d);
	else
		consumer->quote = NULL;

	/* escape character */
	d = slot_getattr(slot, CONSUMER_ATTR_ESCAPE, &isnull);
	if (!isnull)
		consumer->escape = TextDatumGetCString(d);
	else
		consumer->escape = NULL;

	/* now load all brokers */
	consumer->brokers = get_all_brokers();
	MemoryContextSwitchTo(old);

	d = slot_getattr(slot, CONSUMER_ATTR_PARALLELISM, &isnull);
	Assert(!isnull);
	consumer->parallelism = DatumGetInt32(d);

	/* batch size */
	d = slot_getattr(slot, CONSUMER_ATTR_BATCH_SIZE, &isnull);
	Assert(!isnull);
	consumer->batch_size = DatumGetInt32(d);

	/* max bytes */
	d = slot_getattr(slot, CONSUMER_ATTR_MAX_BYTES, &isnull);
	Assert(!isnull);
	consumer->max_bytes = DatumGetInt32(d);

	/* timeout */
	d = slot_getattr(slot, CONSUMER_ATTR_TIMEOUT, &isnull);
	Assert(!isnull);
	consumer->timeout = DatumGetInt32(d);

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	relinfo_close(consumers, NoLock);
}

/*
 * copy_next
 */
static int
copy_next(void *args, void *buf, int minread, int maxread)
{
	StringInfo messages = (StringInfo) args;
	int remaining = messages->len - messages->cursor;
	int read = 0;

	if (maxread <= remaining)
		read = maxread;
	else
		read = remaining;

	if (read == 0)
		return 0;

	memcpy(buf, messages->data + messages->cursor, read);
	messages->cursor += read;

	return read;
}

/*
 * save_consumer_offsets
 */
static void
save_consumer_offsets(KafkaConsumer *consumer, int partition_group)
{
	ScanKeyData skey[1];
	HeapTuple tup = NULL;
	IndexScanDesc scan;
	ResultRelInfo *offsets;
	Datum values[OFFSETS_RELATION_NATTS];
	bool nulls[OFFSETS_RELATION_NATTS];
	bool replace[OFFSETS_RELATION_NATTS];
	bool updated[consumer->num_partitions];
	TupleTableSlot *slot;
	int partition;

	/*
	 * Consumers with a group store offsets in brokers
	 */
	if (consumer->group_id)
	{
		for (partition = 0; partition < consumer->num_partitions; partition++)
		{
			rd_kafka_resp_err_t err;

			if (partition % consumer->parallelism != partition_group)
				continue;

			err = rd_kafka_offset_store(consumer->topic, partition, consumer->offsets[partition]);
			if (err)
			{
				elog(LOG, CONSUMER_LOG_PREFIX "librdkafka error: %s",
						CONSUMER_LOG_PREFIX_PARAMS(consumer), rd_kafka_err2str(err));
			}
		}
	}

	offsets = relinfo_open(get_rangevar(OFFSETS_RELATION), RowExclusiveLock);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(offsets->ri_RelationDesc));

	MemSet(updated, false, sizeof(updated));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(consumer->id));
	scan = index_beginscan(offsets->ri_RelationDesc, offsets->ri_IndexRelationDescs[1],
			GetTransactionSnapshot(), 1, 0);
	index_rescan(scan, skey, 1, NULL, 0);

	/* update any existing offset rows */
	while ((tup = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum d;
		bool isnull;
		int partition;
		int offset;
		HeapTuple modified;

		ExecStoreTuple(tup, slot, InvalidBuffer, false);
		d = slot_getattr(slot, OFFSETS_ATTR_PARTITION, &isnull);
		partition = DatumGetInt32(d);

		/* we only want to update the offsets we're responsible for */
		if (partition % consumer->parallelism != partition_group)
			continue;

		d = slot_getattr(slot, OFFSETS_ATTR_OFFSET, &isnull);
		offset = DatumGetInt64(d);

		updated[partition] = true;

		/* No need to update offset if its unchanged */
		if (offset == consumer->offsets[partition])
			continue;

		MemSet(nulls, false, sizeof(nulls));
		MemSet(replace, false, sizeof(nulls));

		values[OFFSETS_ATTR_OFFSET - 1] = Int64GetDatum(consumer->offsets[partition]);
		replace[OFFSETS_ATTR_OFFSET - 1] = true;

		if (consumer->offsets[partition] == RD_KAFKA_OFFSET_NULL)
			replace[OFFSETS_ATTR_OFFSET - 1] = false;

		modified = heap_modify_tuple(tup, RelationGetDescr(offsets->ri_RelationDesc), values, nulls, replace);
		relinfo_update(offsets, &modified->t_self, modified);
	}

	index_endscan(scan);

	/* now insert any offset rows that didn't already exist */
	for (partition = 0; partition < consumer->num_partitions; partition++)
	{
		if (updated[partition])
			continue;

		if (partition % consumer->parallelism != partition_group)
			continue;

		values[OFFSETS_ATTR_CONSUMER - 1] = ObjectIdGetDatum(consumer->id);
		values[OFFSETS_ATTR_PARTITION - 1] = Int32GetDatum(partition);
		values[OFFSETS_ATTR_OFFSET - 1] = Int64GetDatum(consumer->offsets[partition]);

		MemSet(nulls, false, sizeof(nulls));

		if (consumer->offsets[partition] == RD_KAFKA_OFFSET_NULL)
			nulls[OFFSETS_ATTR_OFFSET - 1] = true;

		tup = heap_form_tuple(RelationGetDescr(offsets->ri_RelationDesc), values, nulls);
		relinfo_insert(offsets, tup);
	}

	ExecDropSingleTupleTableSlot(slot);
	relinfo_close(offsets, NoLock);
}

/*
 * get_copy_statement
 *
 * Get the COPY statement that will be used to write messages to a stream
 */
static CopyStmt *
get_copy_statement(KafkaConsumer *consumer, bool missing_ok)
{
	MemoryContext old = MemoryContextSwitchTo(CacheMemoryContext);
	CopyStmt *stmt = makeNode(CopyStmt);
	Relation rel;
	TupleDesc desc;
	DefElem *format = makeNode(DefElem);
	int i;

	stmt->relation = consumer->rel;
	stmt->filename = NULL;
	stmt->options = NIL;
	stmt->is_from = true;
	stmt->query = NULL;
	stmt->attlist = NIL;

	rel = heap_openrv_extended(consumer->rel, AccessShareLock, missing_ok);
	if (!rel)
		return NULL;

	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		/*
		 * Users can't supply values for arrival_timestamp, so make
		 * sure we exclude it from the copy attr list
		 */
		char *name = pstrdup(NameStr(desc->attrs[i]->attname));
		if (IsStream(RelationGetRelid(rel)) && pg_strcasecmp(name, ARRIVAL_TIMESTAMP) == 0)
			continue;
		stmt->attlist = lappend(stmt->attlist, makeString(name));
	}

	if (consumer->delimiter)
	{
		DefElem *delim = makeNode(DefElem);
		delim->defname = OPTION_DELIMITER;
		delim->arg = (Node *) makeString(consumer->delimiter);
		stmt->options = lappend(stmt->options, delim);
	}

	format->defname = OPTION_FORMAT;
	format->arg = (Node *) makeString(consumer->format);
	stmt->options = lappend(stmt->options, format);

	if (consumer->quote)
	{
		DefElem *quote = makeNode(DefElem);
		quote->defname = OPTION_QUOTE;
		quote->arg = (Node *) makeString(consumer->quote);
		stmt->options = lappend(stmt->options, quote);
	}

	if (consumer->escape)
	{
		DefElem *escape = makeNode(DefElem);
		escape->defname = OPTION_ESCAPE;
		escape->arg = (Node *) makeString(consumer->escape);
		stmt->options = lappend(stmt->options, escape);
	}

	heap_close(rel, NoLock);

	MemoryContextSwitchTo(old);

	return stmt;
}

/*
 * execute_copy
 *
 * Write messages to stream
 */
static void
execute_copy(KafkaConsumer *consumer, KafkaConsumerProc *proc, CopyStmt *stmt, StringInfo buf, int num_messages)
{
	MemoryContext old = CurrentMemoryContext;

	StartTransactionCommand();

	/* we don't want to die in the event of any errors */
	PG_TRY();
	{
		uint64 processed;
		copy_iter_arg = buf;
		DoCopy(stmt, "COPY", &processed);
	}
	PG_CATCH();
	{
		elog(LOG, CONSUMER_LOG_PREFIX "failed to process batch, dropped %d message%s",
				CONSUMER_LOG_PREFIX_PARAMS(consumer), num_messages, (num_messages == 1 ? "" : "s"));

		EmitErrorReport();
		FlushErrorState();

		AbortCurrentTransaction();
	}
	PG_END_TRY();

	if (!IsTransactionState())
		StartTransactionCommand();

	/*
	 * We only store offsets if we're not part of a consumer group.
	 * Consumer groups store their offsets in Kafka.
	 */
	save_consumer_offsets(consumer, proc->partition_group);

	CommitTransactionCommand();

	MemoryContextSwitchTo(old);
}

static void
consume_topic_into_relation(KafkaConsumer *consumer, KafkaConsumerProc *proc, rd_kafka_topic_t *topic)
{
	CopyStmt *copy;
	rd_kafka_message_t **messages;
	MemoryContext work_ctx = CurrentMemoryContext;

	StartTransactionCommand();
	copy = get_copy_statement(consumer, false);
	CommitTransactionCommand();

	messages = MemoryContextAlloc(CacheMemoryContext, sizeof(rd_kafka_message_t *) * consumer->batch_size);

	/*
	 * Consume messages until we are terminated
	 */
	while (!got_SIGTERM)
	{
		int num_consumed;
		int i;
		int messages_buffered = 0;
		int partition;
		char *librdkerrs;
		StringInfo buf;

		MemoryContextSwitchTo(work_ctx);
		MemoryContextReset(work_ctx);

		buf = makeStringInfo();

		for (partition = 0; partition < consumer->num_partitions; partition++)
		{
			if (partition % consumer->parallelism != proc->partition_group)
				continue;

			num_consumed = rd_kafka_consume_batch(topic, partition,
					consumer->timeout, messages, consumer->batch_size);

			if (num_consumed <= 0)
				continue;

			for (i = 0; i < num_consumed; i++)
			{
				rd_kafka_message_t *message = messages[i];

				Assert(message);

				if (message->err)
				{
					/* Ignore partition EOF internal error */
					if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
						elog(LOG, CONSUMER_LOG_PREFIX "librdkafka error: %s",
								CONSUMER_LOG_PREFIX_PARAMS(consumer), rd_kafka_err2str(message->err));
				}
				else if (message->len)
				{
					appendBinaryStringInfo(buf, message->payload, message->len);

					/* COPY expects a newline after each tuple, so add one if missing. */
					if (buf->data[buf->len - 1] != '\n')
						appendStringInfoChar(buf, '\n');

					messages_buffered++;

					Assert(message->offset >= consumer->offsets[partition]);
					consumer->offsets[partition] = message->offset;
				}

				rd_kafka_message_destroy(message);
				messages[i] = NULL;
			}

			/* Flush if we've buffered enough messages or space used by messages has exceeded buffer size threshold */
			if (messages_buffered >= consumer->batch_size || buf->len >= consumer->max_bytes)
			{
				execute_copy(consumer, proc, copy, buf, messages_buffered);
				resetStringInfo(buf);
				messages_buffered = 0;
			}
		}

		librdkerrs = error_buf_pop(&my_error_buf);
		if (librdkerrs)
			elog(LOG, CONSUMER_LOG_PREFIX "librdkafka error: %s",
					CONSUMER_LOG_PREFIX_PARAMS(consumer), librdkerrs);

		if (!messages_buffered)
		{
			pg_usleep(1000);
			continue;
		}

		execute_copy(consumer, proc, copy, buf, messages_buffered);
	}
}

typedef struct
{
	char stream[NAMEDATALEN];
	CopyStmt *copy;
	StringInfo buf;
	int num_messages;
} per_stream_state;

static void
copy_to_streams(KafkaConsumer *consumer, KafkaConsumerProc *proc, HTAB *stream_state_hash)
{
	HASH_SEQ_STATUS scan;
	per_stream_state *state;
	List *to_process = NIL;
	ListCell *lc;

	hash_seq_init(&scan, stream_state_hash);
	while ((state = (per_stream_state *) hash_seq_search(&scan)) != NULL)
	{
		if (!state->num_messages)
			continue;

		to_process = lappend(to_process, state);
	}

	foreach(lc, to_process)
	{
		per_stream_state *state = lfirst(lc);

		execute_copy(consumer, proc, state->copy, state->buf, state->num_messages);
		resetStringInfo(state->buf);
		state->num_messages = 0;
	}
}

static void
consume_topic_stream_partitioned(KafkaConsumer *consumer, KafkaConsumerProc *proc, rd_kafka_topic_t *topic)
{
	rd_kafka_message_t **messages;
	MemoryContext work_ctx = CurrentMemoryContext;
	HASHCTL ctl;
	HTAB *stream_state_hash;

	messages = MemoryContextAlloc(CacheMemoryContext, sizeof(rd_kafka_message_t *) * consumer->batch_size);

	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(per_stream_state);
	ctl.hcxt = CacheMemoryContext;

	stream_state_hash = hash_create("stream_state_hash", 32, &ctl, HASH_ELEM | HASH_CONTEXT);

	/*
	 * Consume messages until we are terminated
	 */
	while (!got_SIGTERM)
	{
		int num_consumed;
		int i;
		int partition;
		char *librdkerrs;
		int messages_buffered = 0;
		Size bytes_buffered = 0;

		MemoryContextSwitchTo(work_ctx);
		MemoryContextReset(work_ctx);

		for (partition = 0; partition < consumer->num_partitions; partition++)
		{
			if (partition % consumer->parallelism != proc->partition_group)
				continue;

			num_consumed = rd_kafka_consume_batch(topic, partition,
					consumer->timeout, messages, consumer->batch_size);

			if (num_consumed <= 0)
				continue;

			for (i = 0; i < num_consumed; i++)
			{
				rd_kafka_message_t *message = messages[i];

				Assert(message);

				if (message->err)
				{
					/* Ignore partition EOF internal error */
					if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
						elog(LOG, CONSUMER_LOG_PREFIX "librdkafka error: %s",
								CONSUMER_LOG_PREFIX_PARAMS(consumer), rd_kafka_err2str(message->err));
				}
				else if (message->key_len && message->len)
				{
					char *key;
					per_stream_state *state;
					bool found;
					StringInfo buf;

					/* Kafka keys aren't null terminated, they're just bytes. So add +1 for null byte at the end */
					key = palloc0(message->key_len + 1);
					memcpy(key, message->key, message->key_len);

					if (strlen(key) != message->key_len)
						elog(LOG, CONSUMER_LOG_PREFIX "key is not a valid string", CONSUMER_LOG_PREFIX_PARAMS(consumer));

					state = hash_search(stream_state_hash, key, HASH_ENTER, &found);
					if (!found)
					{
						MemoryContext old;

						MemSet(state, 0, sizeof(per_stream_state));

						old = MemoryContextSwitchTo(CacheMemoryContext);

						memcpy(state->stream, key, strlen(key));
						state->buf = makeStringInfo();
						state->num_messages = 0;

						MemoryContextSwitchTo(old);
					}

					/*
					 * If we don't have a copy statement, try to get one. We mark get_copy_statement as missing_ok, so even if
					 * the relation is missing, we don't blow up
					 */
					if (!state->copy)
					{
						MemoryContext old;
						List *name_list;

						old = MemoryContextSwitchTo(CacheMemoryContext);
						name_list = textToQualifiedNameList(cstring_to_text(key));

						consumer->rel = makeRangeVarFromNameList(name_list);

						StartTransactionCommand();
						state->copy = get_copy_statement(consumer, true);
						CommitTransactionCommand();

						consumer->rel = NULL;

						MemoryContextSwitchTo(old);
					}

					if (!state->copy)
					{
						elog(WARNING, CONSUMER_LOG_PREFIX "skipping message, relation missing: \"%s\"",
								CONSUMER_LOG_PREFIX_PARAMS(consumer), key);
					}
					else
					{
						buf = state->buf;

						appendBinaryStringInfo(buf, message->payload, message->len);

						/* COPY expects a newline after each tuple, so add one if missing. */
						if (buf->data[buf->len - 1] != '\n')
							appendStringInfoChar(buf, '\n');

						state->num_messages++;
						messages_buffered++;
						bytes_buffered += message->len;

						Assert(message->offset >= consumer->offsets[partition]);
						consumer->offsets[partition] = message->offset;
					}
				}

				rd_kafka_message_destroy(message);
				messages[i] = NULL;
			}

			/* Flush if we've buffered enough messages or space used by messages has exceeded buffer size threshold */
			if (messages_buffered >= consumer->batch_size || bytes_buffered >= consumer->max_bytes)
				copy_to_streams(consumer, proc, stream_state_hash);
		}

		librdkerrs = error_buf_pop(&my_error_buf);
		if (librdkerrs)
			elog(LOG, CONSUMER_LOG_PREFIX "librdkafka error: %s",
					CONSUMER_LOG_PREFIX_PARAMS(consumer), librdkerrs);

		if (!messages_buffered)
		{
			pg_usleep(1000);
			continue;
		}

		copy_to_streams(consumer, proc, stream_state_hash);
	}
}

/*
 * configure_consumer
 */
static void
configure_consumer(rd_kafka_conf_t *conf, rd_kafka_topic_conf_t *topic_conf)
{
	List *opts;
	ListCell *lc;

	Assert(consumer_config);

	if (!SplitIdentifierString(consumer_config, ',', &opts))
		elog(ERROR, "failed to parse pipeline_kafka.consumer_config");

	foreach(lc, opts)
	{
		List *pair;
		char *kv = (char *) lfirst(lc);
		char *k;
		char *v;

		if (!SplitIdentifierString(kv, '=', &pair))
		{
			elog(WARNING, "malformed configuration key-value pair: %s, ignoring", kv);
			continue;
		}

		if (list_length(pair) != 2)
		{
			elog(WARNING, "malformed configuration key-value pair: %s, ignoring", kv);
			continue;
		}

		k = (char *) linitial(pair);
		v = (char *) lsecond(pair);

		if (!strncmp(k, "topic.", strlen("topic.")) == 0)
			rd_kafka_topic_conf_set(topic_conf, k, v, NULL, 0);
		else
			rd_kafka_conf_set(conf, k, v, NULL, 0);
	}
}
/*
 * kafka_consume_main
 *
 * Main function for Kafka consumers running as background workers
 */
void
kafka_consume_main(Datum arg)
{
	char err_msg[512];
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_conf_t *conf;
	const struct rd_kafka_metadata *meta;
	struct rd_kafka_metadata_topic topic_meta;
	rd_kafka_resp_err_t err;
	bool found;
	int32 id = DatumGetInt32(arg);
	ListCell *lc;
	KafkaConsumerProc *proc = hash_search(consumer_procs, &id, HASH_FIND, &found);
	KafkaConsumer consumer;
	int valid_brokers = 0;
	int i;
	int my_partitions = 0;
	MemoryContext work_ctx;
	char errstr[512];
	char val[64];

	MemSet(&consumer, 0, sizeof(KafkaConsumer));
	MemSet(&topic_meta, 0, sizeof(struct rd_kafka_metadata_topic));

	if (!found)
	{
		elog(WARNING, "[kafka consumer] (PID %d) consumer process entry %d not found", MyProcPid, id);
		goto done;
	}

	if (RecoveryInProgress())
	{
		elog(WARNING, "[kafka consumer] (PID %d) cannot cannot consume from Kafka during recovery", MyProcPid);
		goto done;
	}

	pqsignal(SIGTERM, kafka_consume_main_sigterm);
#define BACKTRACE_SEGFAULTS
#ifdef BACKTRACE_SEGFAULTS
	pqsignal(SIGSEGV, debug_segfault);
#endif

	/* we're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* give this proc access to the database */
	BackgroundWorkerInitializeConnectionByOid(proc->db, InvalidOid);

	/* set up error buffer */
	error_buf_init(&my_error_buf, ERROR_BUF_SIZE);

	/* load saved consumer state */
	StartTransactionCommand();
	load_consumer_state(proc->consumer_id, &consumer);

	topic_conf = rd_kafka_topic_conf_new();
	sprintf(val, "%d", consumer.max_bytes);
	rd_kafka_topic_conf_set(topic_conf, "fetch.message.max.bytes", val, errstr, sizeof(errstr));

	conf = rd_kafka_conf_new();
	rd_kafka_conf_set_log_cb(conf, consumer_logger);

	if (broker_version)
		rd_kafka_conf_set(conf, "broker.version.fallback", broker_version, NULL, 0);

	if (consumer.group_id)
	{
		rd_kafka_conf_set(conf, "group.id", consumer.group_id, NULL, 0);
		rd_kafka_conf_set(conf, "offset.store.method", "broker", NULL, 0);
		rd_kafka_conf_set(conf, "auto.commit.enable ", "false", NULL, 0);
		rd_kafka_topic_conf_set(topic_conf, "topic.auto.offset.reset", "latest", NULL, 0);
	}

	/*
	 * Override consumer configuration with anying specified by pipeline_kafka.consumer_config
	 */
	if (consumer_config)
		configure_consumer(conf, topic_conf);

	consumer.kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, err_msg, sizeof(err_msg));

	/*
	 * Add all brokers currently in pipeline_kafka.brokers
	 */
	if (consumer.brokers == NIL)
	{
		elog(WARNING, CONSUMER_LOG_PREFIX "no brokers found in pipeline_kafka.brokers",
				CONSUMER_LOG_PREFIX_PARAMS(&consumer));
		goto done;
	}

	foreach(lc, consumer.brokers)
		valid_brokers += rd_kafka_brokers_add(consumer.kafka, lfirst(lc));

	if (!valid_brokers)
		elog(ERROR, CONSUMER_LOG_PREFIX "no valid brokers were found",
				CONSUMER_LOG_PREFIX_PARAMS(&consumer));

	/*
	 * Set up our topic to read from
	 */
	consumer.topic = rd_kafka_topic_new(consumer.kafka, consumer.topic_name, topic_conf);
	err = rd_kafka_metadata(consumer.kafka, false, consumer.topic, &meta, KAFKA_META_TIMEOUT);

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
		elog(ERROR, CONSUMER_LOG_PREFIX "failed to acquire metadata: %s",
				CONSUMER_LOG_PREFIX_PARAMS(&consumer), rd_kafka_err2str(err));

	Assert(meta->topic_cnt == 1);
	topic_meta = meta->topics[0];
	consumer.num_partitions = topic_meta.partition_cnt;

	load_consumer_offsets(&consumer, &topic_meta, proc->start_offset);
	CommitTransactionCommand();

	/*
	* Begin consuming all partitions that this process is responsible for
	*/
	for (i = 0; i < topic_meta.partition_cnt; i++)
	{
		int partition = topic_meta.partitions[i].id;
		int64_t start_offset;
		int64_t log_start_offset;

		Assert(partition <= consumer.num_partitions);
		if (partition % consumer.parallelism != proc->partition_group)
			continue;

		if (consumer.group_id && proc->start_offset == RD_KAFKA_OFFSET_NULL)
		{
			/*
			 * Query the offsets so we can log them here instead of logging a cryptic RD_KAFKA_OFFSET_STORED
			 * as the start offset
			 */
			rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
			rd_kafka_topic_partition_list_add(topics, consumer.topic_name, partition);
			rd_kafka_committed(consumer.kafka, topics, -1);
			log_start_offset = topics->elems[0].offset;
			start_offset = RD_KAFKA_OFFSET_STORED;
		}
		else
		{
			start_offset = log_start_offset = consumer.offsets[partition];
		}

		elog(LOG, CONSUMER_LOG_PREFIX "consuming partition %d from offset %ld",
				CONSUMER_LOG_PREFIX_PARAMS(&consumer), partition, log_start_offset);

		if (rd_kafka_consume_start(consumer.topic, partition, start_offset) == -1)
			elog(ERROR, CONSUMER_LOG_PREFIX "failed to start consuming: %s",
					CONSUMER_LOG_PREFIX_PARAMS(&consumer), rd_kafka_err2str(rd_kafka_errno2err(errno)));

		my_partitions++;
	}

	if (consumer.group_id)
		elog(LOG, CONSUMER_LOG_PREFIX "group.id is \"%s\"",
				CONSUMER_LOG_PREFIX_PARAMS(&consumer), consumer.group_id);

	/*
	* No point doing anything if we don't have any partitions assigned to us
	*/
	if (my_partitions == 0)
	{
		elog(LOG, CONSUMER_LOG_PREFIX "no partitions to read from", CONSUMER_LOG_PREFIX_PARAMS(&consumer));
		goto done;
	}

	/* set copy hook */
	copy_iter_hook = copy_next;

	work_ctx = AllocSetContextCreate(TopMemoryContext, "KafkaConsumerContext",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(work_ctx);

	if (consumer.rel)
		consume_topic_into_relation(&consumer, proc, consumer.topic);
	else
		consume_topic_stream_partitioned(&consumer, proc, consumer.topic);

done:
	hash_search(consumer_procs, &id, HASH_REMOVE, NULL);

	for (i = 0; i < topic_meta.partition_cnt; i++)
	{
		int partition = topic_meta.partitions[i].id;

		Assert(partition <= consumer.num_partitions);
		if (partition % consumer.parallelism != proc->partition_group)
			continue;

		rd_kafka_consume_stop(consumer.topic, partition);
		elog(LOG, CONSUMER_LOG_PREFIX "stopped consuming partition %d", CONSUMER_LOG_PREFIX_PARAMS(&consumer), partition);
	}

	if (consumer.kafka)
	{
		if (consumer.topic)
			rd_kafka_topic_destroy(consumer.topic);

		rd_kafka_consumer_close(consumer.kafka);
		rd_kafka_destroy(consumer.kafka);
		rd_kafka_wait_destroyed(KAFKA_META_TIMEOUT);
	}
}

/*
 * create_consumer
 *
 * Create a row in pipeline_kafka.consumers representing a topic-relation consumer
 */
static int32
create_or_update_consumer(ResultRelInfo *consumers, text *relation, text *topic,
		text *group_id, text *format, text *delimiter, text *quote, text *escape, int batchsize, int maxbytes,
		int parallelism, int timeout)
{
	HeapTuple tup;
	Datum values[CONSUMER_RELATION_NATTS];
	bool nulls[CONSUMER_RELATION_NATTS];
	int32 consumer_id = 0;
	ScanKeyData skey[2];
	IndexScanDesc scan;
	bool isnull;

	if (!relation)
		relation = cstring_to_text("");

	MemSet(nulls, false, sizeof(nulls));

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(topic));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(relation));

	scan = index_beginscan(consumers->ri_RelationDesc, consumers->ri_IndexRelationDescs[1],
			GetTransactionSnapshot(), 2, 0);
	index_rescan(scan, skey, 2, NULL, 0);
	tup = index_getnext(scan, ForwardScanDirection);

	values[CONSUMER_ATTR_BATCH_SIZE - 1] = Int32GetDatum(batchsize);
	values[CONSUMER_ATTR_MAX_BYTES - 1] = Int32GetDatum(maxbytes);
	values[CONSUMER_ATTR_PARALLELISM - 1] = Int32GetDatum(parallelism);
	values[CONSUMER_ATTR_TIMEOUT - 1] = Int32GetDatum(timeout);
	values[CONSUMER_ATTR_FORMAT - 1] = PointerGetDatum(format);

	if (group_id == NULL)
		nulls[CONSUMER_ATTR_GROUP_ID - 1] = true;
	else
		values[CONSUMER_ATTR_GROUP_ID - 1] = PointerGetDatum(group_id);

	if (delimiter == NULL)
		nulls[CONSUMER_ATTR_DELIMITER - 1] = true;
	else
		values[CONSUMER_ATTR_DELIMITER - 1] = PointerGetDatum(delimiter);

	if (quote == NULL)
		nulls[CONSUMER_ATTR_QUOTE - 1] = true;
	else
		values[CONSUMER_ATTR_QUOTE - 1] = PointerGetDatum(quote);

	if (escape == NULL)
		nulls[CONSUMER_ATTR_ESCAPE - 1] = true;
	else
		values[CONSUMER_ATTR_ESCAPE - 1] = PointerGetDatum(escape);

	if (HeapTupleIsValid(tup))
	{
		/* consumer already exists, so just update it with the given parameters */
		bool replace[CONSUMER_RELATION_NATTS];

		MemSet(replace, true, sizeof(replace));
		replace[CONSUMER_ATTR_ID - 1] = false;
		replace[CONSUMER_ATTR_RELATION - 1] = false;
		replace[CONSUMER_ATTR_TOPIC - 1] = false;

		tup = heap_modify_tuple(tup, RelationGetDescr(consumers->ri_RelationDesc), values, nulls, replace);
		relinfo_update(consumers, &tup->t_self, tup);

		consumer_id = DatumGetInt32(heap_getattr(tup, CONSUMER_ATTR_ID,
				RelationGetDescr(consumers->ri_RelationDesc), &isnull));
		Assert(!isnull);
	}
	else
	{
		Relation seqrel;

		/* consumer doesn't exist yet, create it with the given parameters */
		values[CONSUMER_ATTR_RELATION - 1] = PointerGetDatum(relation);
		values[CONSUMER_ATTR_TOPIC - 1] = PointerGetDatum(topic);

		seqrel = heap_openrv(get_rangevar("consumers_id_seq"), AccessShareLock);
		consumer_id = nextval_internal(RelationGetRelid(seqrel));
		heap_close(seqrel, NoLock);
		values[CONSUMER_ATTR_ID -1] = Int32GetDatum(consumer_id);

		tup = heap_form_tuple(RelationGetDescr(consumers->ri_RelationDesc), values, nulls);
		relinfo_insert(consumers, tup);
	}

	index_endscan(scan);

	CommandCounterIncrement();

	Assert(consumer_id > 0);
	return consumer_id;
}

/*
 * get_consumer_id
 *
 * Get the pipeline_kafka.consumers id for the given relation-topic pair
 *
 */
static int32
get_consumer_id(ResultRelInfo *consumers, text *relation, text *topic)
{
	ScanKeyData skey[2];
	HeapTuple tup = NULL;
	IndexScanDesc scan;
	int32 id = 0;

	if (relation == NULL)
		relation = cstring_to_text("");

	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(topic));
	ScanKeyInit(&skey[1], 2, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(relation));

	scan = index_beginscan(consumers->ri_RelationDesc, consumers->ri_IndexRelationDescs[1],
			GetTransactionSnapshot(), 2, 0);
	index_rescan(scan, skey, 2, NULL, 0);
	tup = index_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
	{
		bool isnull;
		id = DatumGetInt32(heap_getattr(tup, CONSUMER_ATTR_ID,
				RelationGetDescr(consumers->ri_RelationDesc), &isnull));
		Assert(!isnull);
	}

	index_endscan(scan);

	return id;
}

/*
 * launch_consumer_group
 *
 * Launch a group of background worker process that will consume from the given topic
 * into the given relation
 */
static bool
launch_consumer_group(KafkaConsumer *consumer, int64 offset)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	KafkaConsumerGroup *group;
	bool found;
	int i;
	KafkaConsumerGroupKey key;

	key.db = MyDatabaseId;
	key.consumer_id = consumer->id;

	group = (KafkaConsumerGroup *) hash_search(consumer_groups, &key, HASH_ENTER, &found);
	if (found)
	{
		KafkaConsumerProc *proc;
		HASH_SEQ_STATUS iter;
		bool running = false;

		hash_seq_init(&iter, consumer_procs);
		while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
		{
			if (proc->consumer_id == consumer->id && proc->db == MyDatabaseId)
				running = true;
		}

		/* if there are already procs running, it's a noop */
		if (running)
		{
			elog(WARNING, "%s is already being consumed into relation %s",
					consumer->topic_name, consumer->rel ? consumer->rel->relname : "*");
			return true;
		}

		/* no procs actually running, so it's ok to launch new ones */
	}

	group->parallelism = consumer->parallelism;

	for (i = 0; i < group->parallelism; i++)
	{
		/* we just need any unique id here */
		int32 id = rand();
		KafkaConsumerProc *proc;

		proc = (KafkaConsumerProc *) hash_search(consumer_procs, &id, HASH_ENTER, &found);
		if (found)
		{
			i--;
			continue;
		}

		proc->id = id;
		proc->consumer_id = consumer->id;
		proc->partition_group = i;
		proc->start_offset = offset;
		proc->db = MyDatabaseId;

		worker.bgw_main_arg = Int32GetDatum(id);
		worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_SHMEM_ACCESS;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = CONSUMER_WORKER_RESTART_TIME;
		worker.bgw_main = NULL;
		worker.bgw_notify_pid = 0;

		/* this module is loaded dynamically, so we can't use bgw_main */
		sprintf(worker.bgw_library_name, PIPELINE_KAFKA_LIB);
		sprintf(worker.bgw_function_name, KAFKA_CONSUME_MAIN);
		snprintf(worker.bgw_name, BGW_MAXLEN, "[kafka consumer] %s <- %s",
				consumer->rel ? consumer->rel->relname : "*", consumer->topic_name);

		if (!RegisterDynamicBackgroundWorker(&worker, &handle))
			return false;

		proc->worker = *handle;
	}

	return true;
}

/*
 * kafka_consume_begin
 *
 * Begin consuming messages from the given topic into the given relation
 */
PG_FUNCTION_INFO_V1(kafka_consume_begin);
Datum
kafka_consume_begin(PG_FUNCTION_ARGS)
{
	text *topic;
	text *qualified_name;
	RangeVar *relname;
	Relation rel;
	ResultRelInfo *consumers;
	Oid id;
	bool success;
	text *group_id;
	text *format;
	text *delimiter;
	text *quote;
	text *escape;
	int batchsize;
	int maxbytes;
	int parallelism;
	int timeout;
	int64 offset;
	KafkaConsumer consumer;

	check_pipeline_kafka_preloaded();

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "relation cannot be null");
	if (PG_ARGISNULL(3))
		elog(ERROR, "format cannot be null");

	/* there's no point in progressing if there aren't any brokers */
	if (!get_all_brokers())
		elog(ERROR, "add at least one broker with pipeline_kafka.add_broker");

	topic = PG_GETARG_TEXT_P(0);
	qualified_name = PG_GETARG_TEXT_P(1);

	if (PG_ARGISNULL(2))
		group_id = NULL;
	else
		group_id = PG_GETARG_TEXT_P(2);

	format = PG_GETARG_TEXT_P(3);

	if (PG_ARGISNULL(4))
		delimiter = NULL;
	else
		delimiter = PG_GETARG_TEXT_P(4);

	if (pg_strcasecmp(TextDatumGetCString(format), FORMAT_JSON) == 0)
	{
		pfree(format);
		format = (text *) CStringGetTextDatum(FORMAT_CSV);
		if (delimiter != NULL)
			elog(WARNING, "delimiter cannot be specified with format \"json\", ignoring");
		delimiter = (text *)  CStringGetTextDatum(FORMAT_JSON_DELIMITER);
		if (quote != NULL)
			elog(WARNING, "quote cannot be specified with format \"json\", ignoring");
		quote = (text *) CStringGetTextDatum(FORMAT_JSON_QUOTE);

	}

	if (PG_ARGISNULL(5))
		quote = NULL;
	else
		quote = PG_GETARG_TEXT_P(5);

	if (pg_strcasecmp(TextDatumGetCString(format), FORMAT_JSON) == 0)
	{
		pfree(format);
		format = (text *) CStringGetTextDatum(FORMAT_CSV);
		delimiter = (text *)  CStringGetTextDatum(FORMAT_JSON_DELIMITER);
		quote = (text *) CStringGetTextDatum(FORMAT_JSON_QUOTE);
	}

	if (PG_ARGISNULL(6))
		escape = NULL;
	else
		escape = PG_GETARG_TEXT_P(6);

	if (PG_ARGISNULL(7))
		batchsize = 10000;
	else
		batchsize = PG_GETARG_INT32(7);

	if (PG_ARGISNULL(8))
		maxbytes = 32000000;
	else
		maxbytes = PG_GETARG_INT32(8);

	if (PG_ARGISNULL(9))
		parallelism = 1;
	else
		parallelism = PG_GETARG_INT32(9);

	if (PG_ARGISNULL(10))
		timeout = 250;
	else
		timeout = PG_GETARG_INT32(10);

	if (PG_ARGISNULL(11))
		offset = RD_KAFKA_OFFSET_NULL;
	else
		offset = PG_GETARG_INT64(11);

	/* verify that the target relation actually exists */
	relname = makeRangeVarFromNameList(textToQualifiedNameList(qualified_name));
	rel = heap_openrv(relname, AccessShareLock);
	heap_close(rel, NoLock);

	consumers = relinfo_open(get_rangevar(CONSUMER_RELATION), ExclusiveLock);
	id = create_or_update_consumer(consumers, qualified_name, topic, group_id, format,
			delimiter, quote, escape, batchsize, maxbytes, parallelism, timeout);
	load_consumer_state(id, &consumer);
	success = launch_consumer_group(&consumer, offset);
	relinfo_close(consumers, NoLock);

	if (success)
		RETURN_SUCCESS();
	else
		RETURN_FAILURE();
}

/*
 * kafka_consume_end
 *
 * Stop consuming messages from the given topic into the given relation
 */
PG_FUNCTION_INFO_V1(kafka_consume_end);
Datum
kafka_consume_end(PG_FUNCTION_ARGS)
{
	text *topic;
	text *relation;
	ResultRelInfo *consumers;
	int32 id;
	bool found;
	HASH_SEQ_STATUS iter;
	KafkaConsumerProc *proc;
	KafkaConsumerGroupKey key;

	check_pipeline_kafka_preloaded();

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "relation cannot be null");

	topic = PG_GETARG_TEXT_P(0);
	relation = PG_GETARG_TEXT_P(1);
	consumers = relinfo_open(get_rangevar(CONSUMER_RELATION), ExclusiveLock);

	id = get_consumer_id(consumers, relation, topic);
	if (!id)
		elog(ERROR, "there are no consumers for this topic and relation");

	key.consumer_id = id;
	key.db = MyDatabaseId;

	hash_search(consumer_groups, &key, HASH_REMOVE, &found);
	if (!found)
		elog(ERROR, "no consumer processes are running for this topic and relation");

	hash_seq_init(&iter, consumer_procs);
	while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
	{
		if (proc->consumer_id != id && proc->db == MyDatabaseId)
			continue;

		TerminateBackgroundWorker(&proc->worker);
		hash_search(consumer_procs, &id, HASH_REMOVE, NULL);
	}

	relinfo_close(consumers, NoLock);
	RETURN_SUCCESS();
}

/*
 * kafka_consume_begin_all
 *
 * Start all consumers
 */
PG_FUNCTION_INFO_V1(kafka_consume_begin_all);
Datum
kafka_consume_begin_all(PG_FUNCTION_ARGS)
{
	HeapTuple tup = NULL;
	HeapScanDesc scan;
	ResultRelInfo *consumers;

	check_pipeline_kafka_preloaded();

	consumers = relinfo_open(get_rangevar(CONSUMER_RELATION), ExclusiveLock);

	scan = heap_beginscan(consumers->ri_RelationDesc, GetTransactionSnapshot(), 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		bool isnull;
		int32 id = DatumGetInt32(heap_getattr(tup, CONSUMER_ATTR_ID,
				RelationGetDescr(consumers->ri_RelationDesc), &isnull));
		KafkaConsumer consumer;

		Assert(!isnull);

		load_consumer_state(id, &consumer);
		if (!launch_consumer_group(&consumer, RD_KAFKA_OFFSET_END))
			RETURN_FAILURE();
	}

	heap_endscan(scan);
	relinfo_close(consumers, NoLock);

	RETURN_SUCCESS();
}

/*
 * kafka_consume_end_all
 *
 * Stop all consumers
 */
PG_FUNCTION_INFO_V1(kafka_consume_end_all);
Datum
kafka_consume_end_all(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS iter;
	KafkaConsumerProc *proc;
	List *ids = NIL;
	ListCell *lc;
	KafkaConsumerGroupKey key;

	key.db = MyDatabaseId;

	check_pipeline_kafka_preloaded();

	hash_seq_init(&iter, consumer_procs);
	while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
	{
		if (proc->db != MyDatabaseId)
			continue;

		TerminateBackgroundWorker(&proc->worker);

		key.consumer_id = proc->consumer_id;
		hash_search(consumer_groups, &key, HASH_REMOVE, NULL);

		ids = lappend_int(ids, proc->id);
	}

	foreach(lc, ids)
	{
		int32 id = lfirst_int(lc);
		hash_search(consumer_procs, &id, HASH_REMOVE, NULL);
	}

	RETURN_SUCCESS();
}

/*
 * kafka_add_broker
 *
 * Add a broker
 */
PG_FUNCTION_INFO_V1(kafka_add_broker);
Datum
kafka_add_broker(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	Datum values[1];
	bool nulls[1];
	ResultRelInfo *brokers;
	Relation rel;
	text *host;
	ScanKeyData skey[1];
	HeapScanDesc scan;

	check_pipeline_kafka_preloaded();

	if (PG_ARGISNULL(0))
		elog(ERROR, "broker cannot be null");

	host = PG_GETARG_TEXT_P(0);
	brokers = relinfo_open(get_rangevar(BROKER_RELATION), RowExclusiveLock);
	rel = brokers->ri_RelationDesc;

	/* don't allow duplicate brokers */
	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(host));
	scan = heap_beginscan(rel, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tup))
	{
		heap_endscan(scan);
		relinfo_close(brokers, NoLock);
		elog(ERROR, "broker \"%s\" already exists", TextDatumGetCString(host));
	}

	/* broker host */
	values[0] = PointerGetDatum(host);

	MemSet(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	relinfo_insert(brokers, tup);

	heap_endscan(scan);
	relinfo_close(brokers, NoLock);

	if (MyKafka)
		rd_kafka_brokers_add(MyKafka, TextDatumGetCString(host));

	RETURN_SUCCESS();
}

/*
 * kafka_remove_broker
 *
 * Remove a broker
 */
PG_FUNCTION_INFO_V1(kafka_remove_broker);
Datum
kafka_remove_broker(PG_FUNCTION_ARGS)
{
	HeapTuple tup;
	ResultRelInfo *brokers;
	text *host;
	ScanKeyData skey[1];
	HeapScanDesc scan;

	check_pipeline_kafka_preloaded();

	if (PG_ARGISNULL(0))
		elog(ERROR, "broker cannot be null");

	host = PG_GETARG_TEXT_P(0);
	brokers = relinfo_open(get_rangevar(BROKER_RELATION), RowExclusiveLock);

	/* don't allow duplicate brokers */
	ScanKeyInit(&skey[0], 1, BTEqualStrategyNumber, F_TEXTEQ, PointerGetDatum(host));
	scan = heap_beginscan(brokers->ri_RelationDesc, GetTransactionSnapshot(), 1, skey);
	tup = heap_getnext(scan, ForwardScanDirection);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "broker \"%s\" does not exist", TextDatumGetCString(host));

	relinfo_delete(brokers, &tup->t_self);

	heap_endscan(scan);
	relinfo_close(brokers, NoLock);

	RETURN_SUCCESS();
}

/*
 * librdkafka producer logger function
 */
static void
producer_logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
	error_buf_push(&my_error_buf, buf);
}

PG_FUNCTION_INFO_V1(kafka_produce_msg);
Datum
kafka_produce_msg(PG_FUNCTION_ARGS)
{
	char err_msg[512];
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_topic_t *topic;
	char *topic_name;
	bytea *msg;
	int32 partition;
	char *key;
	int keylen;
	char *librdkerrs;

	check_pipeline_kafka_preloaded();

	if (MyKafka == NULL)
	{
		ListCell *lc;
		int valid_brokers = 0;
		rd_kafka_conf_t *conf;
		rd_kafka_t *kafka;
		List *brokers = get_all_brokers();

		if (list_length(brokers) == 0)
			elog(ERROR, "no valid brokers were found");

		error_buf_init(&my_error_buf, ERROR_BUF_SIZE);

		conf = rd_kafka_conf_new();
		rd_kafka_conf_set_log_cb(conf, producer_logger);
		if (broker_version)
			rd_kafka_conf_set(conf, "broker.version.fallback", broker_version, NULL, 0);

		kafka = rd_kafka_new(RD_KAFKA_PRODUCER, conf, err_msg, sizeof(err_msg));

		foreach(lc, brokers)
			valid_brokers += rd_kafka_brokers_add(kafka, lfirst(lc));

		if (valid_brokers == 0)
		{
			rd_kafka_destroy(kafka);
			rd_kafka_wait_destroyed(KAFKA_META_TIMEOUT);
			elog(ERROR, "no valid brokers were found");
		}

		MyKafka = kafka;
	}

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");
	if (PG_ARGISNULL(1))
		elog(ERROR, "message cannot be null");

	topic_name = text_to_cstring(PG_GETARG_TEXT_P(0));
	msg = PG_GETARG_BYTEA_P(1);

	if (PG_NARGS() < 3 || PG_ARGISNULL(2))
		partition = RD_KAFKA_PARTITION_UA;
	else
		partition = PG_GETARG_INT32(2);

	if (PG_NARGS() < 4 || PG_ARGISNULL(3))
	{
		key = NULL;
		keylen = 0;
	}
	else
	{
		key = VARDATA_ANY(PG_GETARG_BYTEA_P(3));
		keylen = VARSIZE_ANY_EXHDR(PG_GETARG_BYTEA_P(3));
	}

	topic_conf = rd_kafka_topic_conf_new();
	topic = rd_kafka_topic_new(MyKafka, topic_name, topic_conf);

	if (rd_kafka_produce(topic, partition, RD_KAFKA_MSG_F_COPY, VARDATA_ANY(msg), VARSIZE_ANY_EXHDR(msg),
			key, keylen, NULL) == -1)
		elog(ERROR, "failed to produce message (size %ld): %s", VARSIZE_ANY_EXHDR(msg), rd_kafka_err2str(rd_kafka_errno2err(errno)));

	rd_kafka_poll(MyKafka, 0);

	librdkerrs = error_buf_pop(&my_error_buf);
	if (librdkerrs)
		elog(LOG, "[pipeline_kafka producer]: %s", librdkerrs);

	RETURN_SUCCESS();
}

PG_FUNCTION_INFO_V1(kafka_emit_tuple);
Datum
kafka_emit_tuple(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger *trig = trigdata->tg_trigger;
	HeapTuple tup;
	TupleDesc desc;
	Datum json;
	char *topic;

	check_pipeline_kafka_preloaded();

	if (trig->tgnargs < 1)
		elog(ERROR, "kafka_emit_tuple: must be provided a topic name");

	if (trig->tgnargs > 3)
		elog(ERROR, "kafka_emit_tuple: only accepts a maximum of 3 arguments");

	/* make sure it's called as a trigger */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("kafka_emit_tuple: must be called as trigger")));

	/* and that it's called on update or insert */
	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && !TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("kafka_emit_tuple: must be called on insert or update")));

	/* and that it's called for each row */
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("kafka_emit_tuple: must be called for each row")));

	/* and that it's called after insert or update */
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("kafka_emit_tuple: must be called after insert or update")));

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		tup = trigdata->tg_newtuple;
	else
		tup = trigdata->tg_trigtuple;

	desc = RelationGetDescr(trigdata->tg_relation);
	topic = trig->tgargs[0];

	json = DirectFunctionCall1(row_to_json, heap_copy_tuple_as_datum(tup, desc));

	if (trig->tgnargs == 1)
		DirectFunctionCall2(kafka_produce_msg, CStringGetTextDatum(topic), json);
	else
	{
		int32 partition = atoi(trig->tgargs[1]);

		if (trig->tgnargs == 2)
			DirectFunctionCall3(kafka_produce_msg, CStringGetTextDatum(topic), json, Int64GetDatum(partition));
		else
		{
			Datum key_name = PointerGetDatum(cstring_to_text(trig->tgargs[2]));
			Datum key_array = PointerGetDatum(construct_array(&key_name, 1, TEXTOID, -1, false, 'i'));
			Datum key;

			PG_TRY();
			{
				key = DirectFunctionCall2(json_extract_path_text, json, key_array);
			}
			PG_CATCH();
			{
				elog(ERROR, "kafka_emit_tuple: tuple has no column \"%s\"", trig->tgargs[2]);
			}
			PG_END_TRY();

			DirectFunctionCall4(kafka_produce_msg, CStringGetTextDatum(topic), json, Int32GetDatum(partition), key);
		}
	}

	return PointerGetDatum(tup);
}

/*
 * kafka_consume_begin_stream_parititioned
 */
PG_FUNCTION_INFO_V1(kafka_consume_begin_stream_partitioned);
Datum
kafka_consume_begin_stream_partitioned(PG_FUNCTION_ARGS)
{
	text *topic;
	ResultRelInfo *consumers;
	Oid id;
	bool success;
	text *group_id;
	text *format;
	text *delimiter;
	text *quote;
	text *escape;
	int batchsize;
	int maxbytes;
	int parallelism;
	int timeout;
	int64 offset;
	KafkaConsumer consumer;

	check_pipeline_kafka_preloaded();

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");
	if (PG_ARGISNULL(2))
		elog(ERROR, "format cannot be null");

	/* there's no point in progressing if there aren't any brokers */
	if (!get_all_brokers())
		elog(ERROR, "add at least one broker with pipeline_kafka.add_broker");

	topic = PG_GETARG_TEXT_P(0);

	if (PG_ARGISNULL(1))
		group_id = NULL;
	else
		group_id = PG_GETARG_TEXT_P(1);

	format = PG_GETARG_TEXT_P(2);

	if (PG_ARGISNULL(3))
		delimiter = NULL;
	else
		delimiter = PG_GETARG_TEXT_P(3);

	if (PG_ARGISNULL(4))
		quote = NULL;
	else
		quote = PG_GETARG_TEXT_P(4);

	if (pg_strcasecmp(TextDatumGetCString(format), FORMAT_JSON) == 0)
	{
		pfree(format);
		format = (text *) CStringGetTextDatum(FORMAT_CSV);
		delimiter = (text *)  CStringGetTextDatum(FORMAT_JSON_DELIMITER);
		quote = (text *) CStringGetTextDatum(FORMAT_JSON_QUOTE);
	}

	if (PG_ARGISNULL(5))
		escape = NULL;
	else
		escape = PG_GETARG_TEXT_P(5);

	if (PG_ARGISNULL(6))
		batchsize = 10000;
	else
		batchsize = PG_GETARG_INT32(6);

	if (PG_ARGISNULL(7))
		maxbytes = 32000000;
	else
		maxbytes = PG_GETARG_INT32(7);

	if (PG_ARGISNULL(8))
		parallelism = 1;
	else
		parallelism = PG_GETARG_INT32(8);

	if (PG_ARGISNULL(9))
		timeout = 250;
	else
		timeout = PG_GETARG_INT32(9);

	if (PG_ARGISNULL(10))
		offset = RD_KAFKA_OFFSET_NULL;
	else
		offset = PG_GETARG_INT64(10);

	consumers = relinfo_open(get_rangevar(CONSUMER_RELATION), ExclusiveLock);
	id = create_or_update_consumer(consumers, NULL, topic, group_id, format,
			delimiter, quote, escape, batchsize, maxbytes, parallelism, timeout);
	load_consumer_state(id, &consumer);
	success = launch_consumer_group(&consumer, offset);
	relinfo_close(consumers, NoLock);

	if (success)
		RETURN_SUCCESS();
	else
		RETURN_FAILURE();
}

/*
 * kafka_consume_end_stream_paritioned
 */
PG_FUNCTION_INFO_V1(kafka_consume_end_stream_partitioned);
Datum
kafka_consume_end_stream_partitioned(PG_FUNCTION_ARGS)
{
	text *topic;
	ResultRelInfo *consumers;
	int32 id;
	bool found;
	HASH_SEQ_STATUS iter;
	KafkaConsumerProc *proc;
	KafkaConsumerGroupKey key;

	check_pipeline_kafka_preloaded();

	if (PG_ARGISNULL(0))
		elog(ERROR, "topic cannot be null");

	topic = PG_GETARG_TEXT_P(0);
	consumers = relinfo_open(get_rangevar(CONSUMER_RELATION), ExclusiveLock);

	id = get_consumer_id(consumers, NULL, topic);
	if (!id)
		elog(ERROR, "there are no consumers for this topic and relation");

	key.consumer_id = id;
	key.db = MyDatabaseId;

	hash_search(consumer_groups, &key, HASH_REMOVE, &found);
	if (!found)
		elog(ERROR, "no consumer processes are running for this topic and relation");

	hash_seq_init(&iter, consumer_procs);
	while ((proc = (KafkaConsumerProc *) hash_seq_search(&iter)) != NULL)
	{
		if (proc->consumer_id != id && proc->db == MyDatabaseId)
			continue;

		TerminateBackgroundWorker(&proc->worker);
		hash_search(consumer_procs, &id, HASH_REMOVE, NULL);
	}

	relinfo_close(consumers, NoLock);
	RETURN_SUCCESS();
}

/*
 * kafka_topic_watermarks
 */
PG_FUNCTION_INFO_V1(kafka_topic_watermarks);
Datum
kafka_topic_watermarks(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	rd_kafka_conf_t *conf;
	text *topic = PG_ARGISNULL(0) ? NULL : PG_GETARG_TEXT_P(0);
	int partition;
	rd_kafka_t *kafka;
	char err_msg[512];
	int64_t low;
	int64_t high;
	char *topic_name;
	ListCell *lc;
	Datum values[3];
	bool nulls[3];
	HeapTuple tup;
	Datum result;

	if (topic == NULL)
		elog(ERROR, "topic must not be null");

	if (PG_ARGISNULL(1))
		elog(ERROR, "partition must not be null");

	partition = PG_GETARG_INT32(1);

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "partition", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "low_watermark", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "high_watermark", INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		topic_name = TextDatumGetCString(topic);
		conf = rd_kafka_conf_new();

		if (broker_version)
			rd_kafka_conf_set(conf, "broker.version.fallback", broker_version, NULL, 0);

		kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, err_msg, sizeof(err_msg));

		foreach(lc, get_all_brokers())
			rd_kafka_brokers_add(kafka, lfirst(lc));

		if (rd_kafka_query_watermark_offsets(kafka, topic_name,
				partition, &low, &high, KAFKA_META_TIMEOUT) != RD_KAFKA_RESP_ERR_NO_ERROR)
			elog(ERROR, "failed to retrieve watermark offsets for partition %d", partition);

		rd_kafka_consumer_close(kafka);
		rd_kafka_destroy(kafka);
		rd_kafka_wait_destroyed(KAFKA_META_TIMEOUT);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = Int32GetDatum(partition);
		values[1] = Int64GetDatum(low);
		values[2] = Int64GetDatum(high);

		tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	SRF_RETURN_DONE(funcctx);
}
