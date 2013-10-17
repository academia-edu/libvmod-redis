#ifndef _LATCH_H_
#define _LATCH_H_

#include <glib.h>

typedef struct Latch {
	guint count;
	GMutex lock;
	GCond cond;
} Latch;

void latch_init( Latch *, guint );
void latch_clear( Latch * );

void latch_await( Latch * );
void latch_trigger( Latch * );

void latch_reset( Latch *, guint );

#endif
