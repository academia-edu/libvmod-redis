#include "latch.h"

#include <glib.h>

#include "vrt.h"
#include "bin/varnishd/cache.h"

#ifndef NDEBUG
#define dbgprintf(sp, ...) VSL(SLT_VCL_trace, ((sp) == NULL ? 0 : ((struct sess *) (sp))->id), __VA_ARGS__)
#else
#define dbgprintf(sp, ...) ((void) (sp))
#endif

void latch_init( Latch *latch, guint count ) {
	g_mutex_init(&latch->lock);
	g_cond_init(&latch->cond);
	latch->count = count;
}

void latch_clear( Latch *latch ) {
	g_mutex_clear(&latch->lock);
	g_cond_clear(&latch->cond);
}

void latch_await( Latch *latch ) {
	g_mutex_lock(&latch->lock);
	dbgprintf(0, "latch_await: %p", latch);
	while( latch->count != 0 )
		g_cond_wait(&latch->cond, &latch->lock);
	g_mutex_unlock(&latch->lock);
}

void latch_trigger( Latch *latch ) {
	g_mutex_lock(&latch->lock);
	dbgprintf(0, "latch_trigger: %p", latch);
	if( latch->count != 0 ) latch->count -= 1;
	if( latch->count == 0 ) g_cond_broadcast(&latch->cond);
	g_mutex_unlock(&latch->lock);
}

void latch_reset( Latch *latch, guint count ) {
	g_mutex_lock(&latch->lock);
	latch->count = count;
	g_mutex_unlock(&latch->lock);
}
