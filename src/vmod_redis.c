#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>

#include <glib.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>

#include "glib-hiredis.h"

#include "vrt.h"
#include "bin/varnishd/cache.h"

#include "vcc_if.h"

#ifndef NDEBUG
#include <stdio.h>
#endif

//#define dbgprintf(...) fprintf(stderr, __VA_ARGS__)
#define dbgprintf(...)

static GRecMutex hiredis_lock;

typedef struct {
	redisAsyncContext *redis;
	GRWLock redis_lock;
	GThread *io_thread;
	GMainContext *io_context;
	GMainLoop *io_loop;
	uint64_t magic;
#define REDIS_MAGIC 0x0be7c29131fbd7a4
} RedisState;

typedef struct {
	GMainContext *io_context;
	GMainLoop *io_loop;
} RedisIoThreadState;

static void free_redis_state( RedisState *rs ) {
	if( rs->redis != NULL ) {
		g_rec_mutex_lock(&hiredis_lock);
		redisAsyncDisconnect(rs->redis);
		g_rec_mutex_unlock(&hiredis_lock);
	}
	g_main_loop_quit(rs->io_loop);
	g_thread_join(rs->io_thread);
	g_thread_unref(rs->io_thread);
	g_main_loop_unref(rs->io_loop);
	g_main_context_unref(rs->io_context);
	g_rw_lock_clear(&rs->redis_lock);
	g_slice_free(RedisState, rs);
}

static void io_thread_main( RedisIoThreadState *ts ) {
	dbgprintf("io_thread_main: begin\n");
	g_main_context_push_thread_default(ts->io_context);
	g_main_loop_run(ts->io_loop);
	g_slice_free(RedisIoThreadState, ts);
	dbgprintf("io_thread_main: end\n");
}

static void init_redis_state( RedisState *rs ) {
	rs->magic = REDIS_MAGIC;
	rs->redis = NULL;
	g_rw_lock_init(&rs->redis_lock);
	rs->io_context = g_main_context_new();
	rs->io_loop = g_main_loop_new(rs->io_context, false);

	RedisIoThreadState *ts = g_slice_new(RedisIoThreadState);
	ts->io_context = rs->io_context,
	ts->io_loop = rs->io_loop;
	rs->io_thread = g_thread_new("vmod-redis-io", (GThreadFunc) io_thread_main, ts);
}

static RedisState *new_redis_state() {
	RedisState *rs = g_slice_new(RedisState);
	init_redis_state(rs);
	return rs;
}

// -- hiredis util functions

typedef struct {
	void (*func)( redisAsyncContext *, redisReply *, void * );
	void *data;
} RedisResponseClosure;

typedef struct {
	RedisState *rs;
	GMutex *done_lock;
	RedisResponseClosure *closure;
} RedisResponseState;

void redis_response_callback( redisAsyncContext *c, redisReply *reply, RedisResponseState *rrs ) {
	(void) c;

	if( reply->type == REDIS_REPLY_ERROR ) {
		dbgprintf("redis_response_callback: error '%s'\n", reply->str);
		g_rw_lock_writer_lock(&rrs->rs->redis_lock);
		rrs->rs->redis = NULL;
		g_rw_lock_writer_unlock(&rrs->rs->redis_lock);
	} else if( rrs->closure != NULL ) {
		g_assert(rrs->closure->func != NULL);
		rrs->closure->func(c, reply, rrs->closure->data);
	}

	g_mutex_unlock(rrs->done_lock);
}

void redis_connect_callback( const redisAsyncContext *c, int status ) {
	(void) c, (void) status;
	dbgprintf("redis_connect_callback: Connected!\n");
}

void redis_disconnect_callback( const redisAsyncContext *c, int status ) {
	(void) status;

	dbgprintf("redis_disconnect_callback: Disconnected!\n");

	RedisState *rs = g_dataset_get_data(c, "redis-state");
	g_assert(rs != NULL);
	g_dataset_remove_data(c, "redis-state");

	g_rec_mutex_lock(&hiredis_lock);
	redisGlibDetach(rs->redis);
	g_rec_mutex_unlock(&hiredis_lock);

	g_rw_lock_writer_lock(&rs->redis_lock);
	rs->redis = NULL;
	g_rw_lock_writer_unlock(&rs->redis_lock);
}

void redis_command( RedisState *rs, RedisResponseClosure *closure, const char *cmd, va_list ap ) {
	if( cmd == NULL ) return;

	g_rw_lock_reader_lock(&rs->redis_lock);
	redisAsyncContext *ctx = rs->redis;
	g_rw_lock_reader_unlock(&rs->redis_lock);

	if( ctx == NULL ) return;

	GMutex done_lock;
	g_mutex_init(&done_lock);

	RedisResponseState rrs = {
		.rs = rs,
		.done_lock = &done_lock,
		.closure = closure
	};

	g_mutex_lock(&done_lock);
	g_rec_mutex_lock(&hiredis_lock);
	redisvAsyncCommand(ctx, (redisCallbackFn *) redis_response_callback, &rrs, cmd, ap);
	g_rec_mutex_unlock(&hiredis_lock);
	dbgprintf("redis_command: waiting...\n");
	g_mutex_lock(&done_lock);
	dbgprintf("redis_command: done\n");

	g_mutex_unlock(&done_lock);
	g_mutex_clear(&done_lock);
}

// -- vmod functions

int vmod_redis_init( struct vmod_priv *global, const struct VCL_conf *conf ) {
	(void) conf;
	global->priv = new_redis_state();
	global->free = (vmod_priv_free_f *) free_redis_state;
	return 0;
}

static RedisState *redis_state( struct vmod_priv *global ) {
	g_assert(global->priv != NULL);
	RedisState *ret = (RedisState *) global->priv;
	g_assert(ret->magic == REDIS_MAGIC);
	return ret;
}

void vmod_connect( struct sess *sp, struct vmod_priv *global, const char *host, int port ) {
	(void) sp;

	dbgprintf("vmod_connect: host = '%s', port = %d\n", host, port);

	RedisState *rs = redis_state(global);
	g_rw_lock_writer_lock(&rs->redis_lock);

	if( rs->redis != NULL ) {
		g_rw_lock_writer_unlock(&rs->redis_lock);
		return;
	}

	g_rec_mutex_lock(&hiredis_lock);
	rs->redis = redisAsyncConnect(host, port);
	redisAsyncSetConnectCallback(rs->redis, redis_connect_callback);
	redisAsyncSetDisconnectCallback(rs->redis, redis_disconnect_callback);
	g_rec_mutex_unlock(&hiredis_lock);

	g_rw_lock_writer_unlock(&rs->redis_lock);

	g_dataset_set_data(rs->redis, "redis-state", rs);

	g_rec_mutex_lock(&hiredis_lock);
	redisGlibAttach(rs->io_context, rs->redis);
	g_rec_mutex_unlock(&hiredis_lock);
}

void vmod_disconnect( struct sess *sp, struct vmod_priv *global ) {
	(void) sp;
	dbgprintf("vmod_disconnect\n");
	RedisState *rs = redis_state(global);
	g_rw_lock_reader_lock(&rs->redis_lock);
	redisAsyncContext *ctx = rs->redis;
	g_rw_lock_reader_unlock(&rs->redis_lock);
	g_rec_mutex_lock(&hiredis_lock);
	redisAsyncDisconnect(ctx);
	g_rec_mutex_unlock(&hiredis_lock);
}

void vmod_command_void( struct sess *sp, struct vmod_priv *global, const char *cmd, ...) {
	(void) sp;
	dbgprintf("vmod_command_void: cmd = '%s'\n", cmd);
	RedisState *rs = redis_state(global);
	va_list ap;
	va_start(ap, cmd);
	redis_command(rs, NULL, cmd, ap);
	va_end(ap);
}

static void vmod_command_int_callback( redisAsyncContext *rac, redisReply *reply, int *out ) {
	(void) rac;
	if( reply != NULL && reply->type == REDIS_REPLY_INTEGER ) *out = reply->integer;
}

int vmod_command_int( struct sess *sp, struct vmod_priv *global, const char *cmd, ... ) {
	(void) sp;
	dbgprintf("vmod_command_int: cmd = '%s'\n", cmd);
	RedisState *rs = redis_state(global);
	int val = -1;
	RedisResponseClosure rrc = {
		.func = (__typeof__(rrc.func)) vmod_command_int_callback,
		.data = &val
	};
	va_list ap;
	va_start(ap, cmd);
	redis_command(rs, &rrc, cmd, ap);
	va_end(ap);
	return val;
}

typedef struct {
	struct sess *sp;
	char *ret;
} VmodCommandStringCallbackArgs;

static void vmod_command_string_callback( redisAsyncContext *rac, redisReply *reply, VmodCommandStringCallbackArgs *args ) {
	(void) rac;
#ifndef NDEBUG
	dbgprintf("vmod_command_string_callback: reply = %p\n", reply);
	if( reply != NULL ) {
		dbgprintf("vmod_command_string_callback: reply->type = %d, REDIS_REPLY_STRING = %d\n", reply->type, REDIS_REPLY_STRING);
	}
#endif
	if( reply != NULL && reply->type == REDIS_REPLY_STRING )
		args->ret = WS_Dup(args->sp->wrk->ws, reply->str);
}

const char * vmod_command_string(struct sess *sp, struct vmod_priv *global, const char *cmd, ...) {
	(void) sp;
	dbgprintf("vmod_command_string: cmd = '%s'\n", cmd);
	RedisState *rs = redis_state(global);
	VmodCommandStringCallbackArgs args = {
		.sp = sp,
		.ret = NULL
	};
	RedisResponseClosure rrc = {
		.func = (__typeof__(rrc.func)) vmod_command_string_callback,
		.data = &args
	};
	va_list ap;
	va_start(ap, cmd);
	redis_command(rs, &rrc, cmd, ap);
	va_end(ap);
	dbgprintf("vmod_command_string: args.ret = '%s'\n", args.ret);
	return args.ret;
}
