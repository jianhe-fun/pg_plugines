/* -------------------------------------------------------------------------
 *
 * worker_spi.c
 *		Sample background worker code that demonstrates various coding
 *		patterns: establishing a database connection; starting and committing
 *		transactions; using GUC variables, and heeding SIGHUP to reread
 *		the configuration file; reporting to pg_stat_activity; using the
 *		process latch to sleep and exit in case of postmaster death.
 *
 * This code connects to a database, creates a schema and table, and on a interval basics
 * query the system client non-idle state, and insert it to a table.
 * So you can based on that table calculate each connection online time.
 * online time: for every X interval second, if the client state is not idle.
 * then you can use gap and island logic, calcualte one client total online time.
 *
 * gap and island: https://stackoverflow.com/questions/tagged/gaps-and-islands+postgresql
 *
 * You do not need add it processing of shared_preload_libraries, but if you add it,
 * it will be run when server starts.
 * currently only allow one background worker do the spi logic. but it can be do via multiple worker.
 * Adding multiple background workers seems does not make sense in here.
 * function worker_spi_launch can be called by superuser.
 * /home/jian/postgres/regression5/lib/x86_64-linux-gnu
 * 	
 *  Installation 
	make PG_CONFIG=/your/full/pg_config binary path/
	make install PG_CONFIG=/your/full/pg_config binary path/
	
	use it in psql session:
	DROP EXTENSION IF EXISTS worker_spi CASCADE;
	CREATE EXTENSION worker_spi;
	SELECT worker_spi_launch(1,16384, 10);

 	idea comes from:
	https://git.postgresql.org/cgit/postgresql.git/tree/src/test/modules/worker_spi/worker_spi.c
 	https://github.com/michaelpq/pg_plugins/blob/main/kill_idle/kill_idle.c
 	https://www.postgresql.org/docs/current/bgworker.html
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(worker_spi_launch);

PGDLLEXPORT void worker_spi_main(Datum main_arg) pg_attribute_noreturn();

/* GUC variables */
static int	worker_spi_naptime = 10;
static char *worker_spi_role = NULL;
static char *worker_spi_database = NULL;

/* value cached, fetched from shared memory */
static uint32 worker_spi_wait_event_main = 0;

typedef struct worktable
{
	const char *schema;
	const char *name;
} worktable;

/*
 * Initialize workspace for a worker process: create the table public.connection_stat if it doesn't
 * already exist.
 */
static void
initialize_worker_spi(worktable *table)
{
	int			ret;
	int			ntup;
	bool		isnull;
	StringInfoData buf;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing worker_spi module");

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT COUNT(*) FROM pg_namespace pn "
    "JOIN pg_class pc ON pn.oid = pc.relnamespace "
    "WHERE pn.nspname = '%s' AND pc.relname = '%s'", table->schema, table->name);

	debug_query_string = buf.data;
	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));
	if (isnull)
		elog(FATAL, "null result");
	/* if not exists, then create the table. */
	if (ntup == 0)
	{
		debug_query_string = NULL;
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE SCHEMA \"%s\" "
						 "CREATE TABLE \"%s\" ( "
                         "state text, inserted_at timestamptz, "
						 "application_name text,usesysid oid)",
						 table->schema, table->name);

		/* set statement start time */
		SetCurrentStatementStartTimestamp();

		debug_query_string = buf.data;
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create my schema");

		debug_query_string = NULL;	/* rest is not statement-specific */
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	debug_query_string = NULL;
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
worker_spi_main(Datum main_arg)
{
	StringInfoData buf;
	Oid			dboid;
	Oid			roleoid;
	char	   *p;
	bits32		flags = 0;
	worktable  *table;

	/* fetch database and role OIDs, these are set for a dynamic worker */
	p = MyBgworkerEntry->bgw_extra;
	memcpy(&dboid, p, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(&roleoid, p, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(&flags, p, sizeof(bits32));

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	if (OidIsValid(dboid))
		BackgroundWorkerInitializeConnectionByOid(dboid, roleoid, flags);

	/*
	 * Disable parallel query for workers started with BYPASS_ALLOWCONN or
	 * BGWORKER_BYPASS_ALLOWCONN so as these don't attempt connections using a
	 * database or a role that may not allow that.
	 */
	if ((flags & (BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ROLELOGINCHECK)))
		SetConfigOption("max_parallel_workers_per_gather", "0",
						PGC_USERSET, PGC_S_OVERRIDE);

	elog(LOG, "initialized with %s", MyBgworkerEntry->bgw_name);

	table = palloc(sizeof(worktable));
	table->schema = pstrdup("test");
	table->name = pstrdup("connection_stat");

    initialize_worker_spi(table);
	initStringInfo(&buf);
	appendStringInfo(&buf,
                     "INSERT INTO %s.%s (state, inserted_at,usesysid, application_name) "
                     "SELECT state, now(), usesysid, "
					 "CASE WHEN length(application_name) = 0 "
					 "THEN (TO_CHAR(now(), 'YYYY_MM_DD_') ||  pg_backend_pid()::text) "
					 "ELSE application_name END "
					 "FROM pg_stat_activity "
                     "WHERE backend_type = 'client backend' "
                     "AND pid <> pg_backend_pid() "
                     "AND state != 'idle' ",
					 table->schema, table->name);
	/*
	 * Main loop: do this until SIGTERM is received and processed by
	 * ProcessInterrupts.
	 */
	for (;;)
	{
		int			ret;

		/* First time, allocate or get the custom wait event */
		if (worker_spi_wait_event_main == 0)
			worker_spi_wait_event_main = WaitEventExtensionNew("WorkerSpiMain");

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 worker_spi_naptime * 1000L,
						 worker_spi_wait_event_main);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Start a transaction on which we can run queries.  Note that each
		 * StartTransactionCommand() call should be preceded by a
		 * SetCurrentStatementStartTimestamp() call, which sets both the time
		 * for the statement we're about the run, and also the transaction
		 * start time.  Also, each other query sent to SPI should probably be
		 * preceded by SetCurrentStatementStartTimestamp(), so that statement
		 * start time is always up to date.
		 *
		 * The SPI_connect() call lets us run queries through the SPI manager,
		 * and the PushActiveSnapshot() call creates an "active" snapshot
		 * which is necessary for queries to have MVCC data to work on.
		 *
		 * The pgstat_report_activity() call makes our activity visible
		 * through the pgstat views.
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		debug_query_string = buf.data;
		pgstat_report_activity(STATE_RUNNING, buf.data);

		/* We can now execute queries via SPI */
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_INSERT)
			elog(FATAL, "cannot insert to %s.%s", table->schema, table->name);
		/*
		 * And finish our transaction.
		 */
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		debug_query_string = NULL;
		pgstat_report_stat(true);
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	/* Not reachable */
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	/* get the configuration */

	/*
	 * These GUCs are defined even if this library is not loaded with
	 * shared_preload_libraries, for worker_spi_launch().
	 */
	DefineCustomIntVariable("worker_spi.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&worker_spi_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("worker_spi.role",
							   "Role to connect with.",
							   NULL,
							   &worker_spi_role,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;
	MarkGUCPrefixReserved("worker_spi");
}

/*
 * Dynamically launch an SPI worker.
 */
Datum
worker_spi_launch(PG_FUNCTION_ARGS)
{
	int32		i = PG_GETARG_INT32(0); 	/* demo only, only only set to one. */
	Oid			dboid = PG_GETARG_OID(1);
	Oid			roleoid = PG_GETARG_OID(2);
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	char	   *p;
	bits32		flags = 0;

    if (i != 1)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("only one background worker is allowed")));

	if (!superuser_arg(roleoid))
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 errmsg("must be superuser to launch dynamic background worker")));

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;	/* indicating not to restart the process in case of a crash.*/
	
	/* bgw_library_name is the name of a library
	* in which the initial entry point 
	* for the background worker should be sought. 
	*/
	sprintf(worker.bgw_library_name, "worker_spi");	

	/*
	 * bgw_function_name is the name of the function to use
	 * as the initial entry point for the new background worker. 
	 * If this function is in a dynamically loaded library, 
	 * it must be marked PGDLLEXPORT (and not static).
	*/
	sprintf(worker.bgw_function_name, "worker_spi_main");
	
	/*
	 * bgw_name and bgw_type are strings to be used in 
	 * log messages, process listings and similar contexts. 
	 * bgw_type should be the same for all background workers of the same type, 
	 * so that it is possible to group such workers in a process listing, 
	 * 
	 * bgw_name on the other hand can contain additional information about the specific process. 
	 * (Typically, the string for bgw_name will contain the type somehow,
	 * but that is not strictly required.)
	*/
	snprintf(worker.bgw_name, BGW_MAXLEN, "worker_spi dynamic worker %d", i);
	snprintf(worker.bgw_type, BGW_MAXLEN, "worker_spi dynamic");
	worker.bgw_main_arg = Int32GetDatum(i);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	/*
	 * Register database and role to use for the worker started in bgw_extra.
	 * If none have been provided, this will fall back to the GUCs at startup.
	 */
	if (!OidIsValid(dboid))
		dboid = get_database_oid(worker_spi_database, false);

	/*
	 * worker_spi_role is NULL by default, so this gives to worker_spi_main()
	 * an invalid OID in this case.
	 */
	if (!OidIsValid(roleoid) && worker_spi_role)
		roleoid = get_role_oid(worker_spi_role, false);

	p = worker.bgw_extra;
	memcpy(p, &dboid, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(p, &roleoid, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(p, &flags, sizeof(bits32));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}
