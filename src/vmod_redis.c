#include <config.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>

#include <glib.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>

#include "glib-hiredis.h"
#include "latch.h"

#include "vrt.h"
#include "bin/varnishd/cache.h"

#include "vcc_if.h"

#ifndef NDEBUG
#define dbgprintf(sp, ...) VSL(SLT_VCL_trace, ((sp) == NULL ? 0 : ((struct sess *) (sp))->id), __VA_ARGS__)
#else
#define dbgprintf(sp, ...) ((void) sizeof(sp))
#endif

GRecMutex hiredis_lock;

typedef struct {
	uint64_t magic;
#define REDIS_MAGIC 0x0be7c29131fbd7a4ULL
	struct {
		volatile redisAsyncContext *context;
		char *host;
		int port;
		gint connected;
		bool watchdog_running;
	} redis;
	GThread *io_thread;
	GMainContext *io_context;
	GMainLoop *io_loop;
} RedisState;

typedef struct {
	GMainContext *io_context;
	GMainLoop *io_loop;
} RedisIoThreadState;

static void redis_disconnect( RedisState *, bool );

static void io_thread_main( RedisIoThreadState *ts ) {
	dbgprintf(0, "io_thread_main: begin");
	g_main_context_push_thread_default(ts->io_context);
	g_main_loop_run(ts->io_loop);
	g_slice_free(RedisIoThreadState, ts);
	dbgprintf(0, "io_thread_main: end");
}

static void free_redis_state( RedisState *rs ) {
	if( rs->redis.context != NULL ) redis_disconnect(rs, false);
	g_main_loop_quit(rs->io_loop);
	g_thread_join(rs->io_thread);
	g_thread_unref(rs->io_thread);
	g_main_loop_unref(rs->io_loop);
	g_main_context_unref(rs->io_context);
	g_free(rs->redis.host);
	g_slice_free(RedisState, rs);
}

static void init_redis_state( RedisState *rs ) {
	rs->magic = REDIS_MAGIC;
	rs->redis.context = NULL;
	rs->redis.host = NULL;
	rs->redis.port = 0;
	rs->io_context = g_main_context_new();
	rs->io_loop = g_main_loop_new(rs->io_context, false);

	RedisIoThreadState *ts = g_slice_new(RedisIoThreadState);
	ts->io_context = rs->io_context;
	ts->io_loop = rs->io_loop;
	rs->io_thread = g_thread_new("vmod-redis-io", (GThreadFunc) io_thread_main, ts);
}

static RedisState *new_redis_state() {
	RedisState *rs = g_slice_new(RedisState);
	init_redis_state(rs);
	return rs;
}

// -- util functions

static bool contains_null_strings( const char *cmd, va_list ap ) {
	if( cmd == NULL ) return true;

	const char *s = NULL;
	do {
		s = va_arg(ap, const char *);
		if( s == NULL ) {
			VSL(SLT_VCL_error, 0, "contains_null_strings: Found NULL string arguments");
			return true;
		}
	} while( s != vrt_magic_string_end );

	return false;
}

// -- hiredis util functions

typedef struct {
	void (*func)( redisAsyncContext *, redisReply *, void * );
	void *data;
} RedisResponseClosure;

static void redis_connect_callback( const redisAsyncContext * );
static void redis_disconnect_callback( const redisAsyncContext *, int );
void redis_command( RedisState *, RedisResponseClosure *, bool, const char *, ... );

static void redis_connect( RedisState *rs, bool async ) {
	dbgprintf(
		0,
		"redis_connect: host = '%s', port = %d, async = %s",
		rs->redis.host,
		rs->redis.port,
		async ? "true" : "false"
	);

	g_rec_mutex_lock(&hiredis_lock);
	redisAsyncContext *context = (redisAsyncContext *) g_atomic_pointer_get(&rs->redis.context);
	if( context != NULL ) return;

	context = redisAsyncConnect(rs->redis.host, rs->redis.port);
	g_atomic_pointer_set(&rs->redis.context, context);
	g_dataset_set_data(context, "redis-state", rs);
	redisAsyncSetConnectCallback(context, (redisConnectCallback *) redis_connect_callback);
	redisAsyncSetDisconnectCallback(context, redis_disconnect_callback);
	redisGlibAttach(rs->io_context, context);
	g_rec_mutex_unlock(&hiredis_lock);

	redis_command(rs, NULL, async, "PING");
}

typedef struct {
	RedisState *rs;
	Latch *done;
	RedisResponseClosure *closure;
} RedisResponseState;

static void redis_response_callback( redisAsyncContext *c, redisReply *reply, RedisResponseState *rrs ) {
	(void) c;

	dbgprintf(0, "redis_response_callback: rrs = %p", (void *) rrs);

	if( reply == NULL ) {
		if( c->err == REDIS_ERR ) {
			VSL(SLT_VCL_error, 0, "redis_response_callback: error '%s'", c->errstr);
		} else {
			VSL(SLT_VCL_error, 0, "redis_response_callback: unspecified error");
		}
	} else if( reply->type == REDIS_REPLY_ERROR ) {
		VSL(SLT_VCL_error, 0, "redis_response_callback: error '%s'", reply->str);
	} else if( rrs->closure != NULL ) {
		g_assert(rrs->closure->func != NULL);
		rrs->closure->func(c, reply, rrs->closure->data);
	}

	dbgprintf(0, "redis_response_callback: rrs->done = %p", rrs->done);
	if( rrs->done != NULL ) latch_trigger(rrs->done);
	g_slice_free(RedisResponseState, rrs);
}

static gboolean redis_reconnect_callback( gpointer data ) {
	RedisState *rs = (RedisState *) data;
	dbgprintf(
		0,
		"redis_reconnect_callback: rs->redis.connected = %s",
		g_atomic_int_get(&rs->redis.connected) ? "true" : "false"
	);
	if( !g_atomic_int_get(&rs->redis.connected) ) {
		redis_connect(rs, true);
	}
	return G_SOURCE_CONTINUE;
}

static void redis_connect_callback( const redisAsyncContext *c ) {
	dbgprintf(0, "redis_connect_callback: begin");

	RedisState *rs = NULL;
	// Busy wait for the RedisState to appear
	do {
		rs = g_dataset_get_data(c, "redis-state");
		if( rs == NULL ) {
			dbgprintf(0, "rs is null. c = %p", (void *) c);
			g_thread_yield();
		}
	} while( rs == NULL );

	__typeof__(c->err) err = c->err;
	dbgprintf(0, "redis_connect_callback: c->err = %d", err);
	if( err != REDIS_OK ) {
		VSL(SLT_VCL_error, 0, "redis_connect_callack: error '%s'", c->errstr);

		g_dataset_remove_data(c, "redis-state");

		g_atomic_pointer_set(&rs->redis.context, NULL);
		g_atomic_int_set(&rs->redis.connected, true);

		redis_connect(rs, true);
	} else {
		dbgprintf(0, "redis_connect_callback: Connected");

		if( !rs->redis.watchdog_running ) {
			rs->redis.watchdog_running = true;
			g_timeout_add_seconds(5, redis_reconnect_callback, rs);
		}
	}
}

static void redis_disconnect_callback( const redisAsyncContext *c, int status ) {
	VSL(SLT_VCL_error, 0, "redis_disconnect_callack: BEGIN");

	if( status == REDIS_ERR || c->err == REDIS_ERR ) {
		VSL(SLT_VCL_error, 0, "redis_disconnect_callack: error '%s'", c->errstr);
	} else {
		dbgprintf(0, "redis_disconnect_callback: Disconnected");
	}

	RedisState *rs = g_dataset_get_data(c, "redis-state");
	if( rs == NULL ) return;
	g_dataset_remove_data(c, "redis-state");

	G_STATIC_ASSERT(sizeof(gsize) >= sizeof(gpointer));
	// This is an atomic version of context = rs->redis.context; rs->redis.context = NULL
	redisAsyncContext *context = (redisAsyncContext *) g_atomic_pointer_and(&rs->redis.context, 0);

	if( status == REDIS_ERR || c->err == REDIS_ERR ) {
		g_atomic_int_set(&rs->redis.connected, false);
		redis_connect(rs, true);
	} else {
		Latch *done = g_dataset_get_data(context, "redis-disconnect-latch");
		if( done != NULL ) latch_trigger(done);
	}
}

static void redis_disconnect( RedisState *rs, bool async ) {
	Latch done;
	if( !async )
		latch_init(&done, 1);

	g_rec_mutex_lock(&hiredis_lock);
	redisAsyncContext *context = g_atomic_pointer_get(&rs->redis.context);

	if( !async ) g_dataset_set_data(context, "redis-disconnect-latch", &done);

	redisAsyncDisconnect(context);
	g_rec_mutex_unlock(&hiredis_lock);

	if( !async ) {
		latch_await(&done);
		g_dataset_remove_data(context, "redis-disconnect-latch");
		latch_clear(&done);
	}
}

void redis_vcommand( RedisState *rs, RedisResponseClosure *closure, bool async, const char *cmd, va_list ap ) {
	if( cmd == NULL ) return;

	Latch done;
	if( !async )
		latch_init(&done, 1);

	RedisResponseState *rrs = g_slice_new(RedisResponseState);
	dbgprintf(0, "redis_vcommand: rrs = %p, cmd = '%s'", (void *) rrs, cmd);
	rrs->rs = rs;
	rrs->closure = closure;
	if( async ) {
		rrs->done = NULL;
	} else {
		rrs->done = &done;
	}


	bool skip_wait = false;
	g_rec_mutex_lock(&hiredis_lock);
	redisAsyncContext *context = g_atomic_pointer_get(&rs->redis.context);
	if( context == NULL ) {
		skip_wait = true;
	} else {
		redisvAsyncCommand(context, (redisCallbackFn *) redis_response_callback, rrs, cmd, ap);
	}
	g_rec_mutex_unlock(&hiredis_lock);

	if( skip_wait ) {
		VSL(SLT_VCL_trace, 0, "redis_vcommand: Not connected");
	}

	if( !async ) {
		if( !skip_wait ) {
			dbgprintf(0, "redis_vcommand: Waiting...");
			latch_await(&done);
			dbgprintf(0, "redis_vcommand: Done");
		}
		latch_clear(&done);
	}
}

void redis_command( RedisState *rs, RedisResponseClosure *closure, bool async, const char *cmd, ... ) {
	va_list ap;
	va_start(ap, cmd);
	redis_vcommand(rs, closure, async, cmd, ap);
	va_end(ap);
}

// -- vmod functions

static void vmod_redis_free( RedisState *rs ) {
	free_redis_state(rs);
}

int vmod_redis_init( struct vmod_priv *global, const struct VCL_conf *conf ) {
	(void) conf;
	dbgprintf(0, "vmod_redis_init");

	global->priv = new_redis_state();
	global->free = (vmod_priv_free_f *) vmod_redis_free;
	return 0;
}

static RedisState *redis_state( struct vmod_priv *global ) {
	g_assert(global->priv != NULL);
	RedisState *ret = (RedisState *) global->priv;
	g_assert(ret->magic == REDIS_MAGIC);
	return ret;
}

void vmod_disconnect( struct sess *sp, struct vmod_priv *global ) {
	dbgprintf(sp, "vmod_disconnect");

	redis_disconnect(redis_state(global), false);
}

void vmod_command_async( struct sess *sp, struct vmod_priv *global, const char *cmd, ... ) {
	g_return_if_fail(cmd != NULL);
	dbgprintf(sp, "vmod_command_async: cmd = '%s'", cmd);

	RedisState *rs = redis_state(global);

	va_list ap, ap2;
	va_start(ap, cmd);
	va_copy(ap2, ap);

	if( !contains_null_strings(cmd, ap) )
		redis_vcommand(rs, NULL, true, cmd, ap2);

	va_end(ap);
	va_end(ap2);
}

void vmod_command_void( struct sess *sp, struct vmod_priv *global, const char *cmd, ... ) {
	g_return_if_fail(cmd != NULL);
	dbgprintf(sp, "vmod_command_void: cmd = '%s'", cmd);

	RedisState *rs = redis_state(global);

	va_list ap, ap2;
	va_start(ap, cmd);
	va_copy(ap2, ap);

	if( !contains_null_strings(cmd, ap) )
		redis_vcommand(rs, NULL, false, cmd, ap2);

	va_end(ap);
	va_end(ap2);
}

void vmod_connect( struct sess *sp, struct vmod_priv *global, const char *host, int port ) {
	g_return_if_fail(host != NULL);

	dbgprintf(sp, "vmod_connect: host = '%s', port = %d", host, port);

	RedisState *rs = redis_state(global);
	g_assert(rs->redis.host == NULL && "vmod_connect called multiple times!");
	rs->redis.host = g_strdup(host);
	rs->redis.port = port;

	redis_connect(rs, false);
}

static void vmod_command_int_callback( redisAsyncContext *rac, redisReply *reply, int *out ) {
	(void) rac;
	if( reply != NULL && reply->type == REDIS_REPLY_INTEGER ) *out = reply->integer;
}

int vmod_command_int( struct sess *sp, struct vmod_priv *global, const char *cmd, ... ) {
	g_return_val_if_fail(cmd != NULL, -1);
	dbgprintf(sp, "vmod_command_int: cmd = '%s'", cmd);

	RedisState *rs = redis_state(global);

	int val = -1;
	RedisResponseClosure rrc = {
		.func = (__typeof__(rrc.func)) vmod_command_int_callback,
		.data = &val
	};

	va_list ap, ap2;
	va_start(ap, cmd);
	va_copy(ap2, ap);

	if( !contains_null_strings(cmd, ap) )
		redis_vcommand(rs, &rrc, false, cmd, ap2);

	va_end(ap);
	va_end(ap2);
	return val;
}

typedef struct {
	struct sess *sp;
	char *ret;
} VmodCommandStringCallbackArgs;

static void vmod_command_string_callback( redisAsyncContext *rac, redisReply *reply, VmodCommandStringCallbackArgs *args ) {
	(void) rac;
#ifndef NDEBUG
	dbgprintf(0, "vmod_command_string_callback: reply = %p", (void *) reply);
	if( reply != NULL ) {
		dbgprintf(0, "vmod_command_string_callback: reply->type = %d, REDIS_REPLY_STRING = %d", reply->type, REDIS_REPLY_STRING);
	}
#endif
	if( reply != NULL && reply->type == REDIS_REPLY_STRING )
		args->ret = WS_Dup(args->sp->wrk->ws, reply->str);
}

const char * vmod_command_string(struct sess *sp, struct vmod_priv *global, const char *cmd, ...) {
	g_return_val_if_fail(cmd != NULL, "");
	dbgprintf(0, "vmod_command_string: cmd = '%s'", cmd);

	RedisState *rs = redis_state(global);

	VmodCommandStringCallbackArgs args = {
		.sp = sp,
		.ret = NULL
	};
	RedisResponseClosure rrc = {
		.func = (__typeof__(rrc.func)) vmod_command_string_callback,
		.data = &args
	};

	va_list ap, ap2;
	va_start(ap, cmd);
	va_copy(ap2, ap);

	if( !contains_null_strings(cmd, ap) )
		redis_vcommand(rs, &rrc, false, cmd, ap2);

	va_end(ap);
	va_end(ap2);

	dbgprintf(0, "vmod_command_string: args.ret = '%s'", args.ret);
	return args.ret;
}
