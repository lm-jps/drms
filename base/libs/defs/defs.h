#ifndef _DEFS_H
#define _DEFS_H

#include "hcontainer.h"

#ifdef DRMSSTRING
#undef DRMSSTRING
#endif

#ifdef REGISTERSTRINGSPREFIX
#undef REGISTERSTRINGSPREFIX
#endif

#ifdef REGISTERSTRINGSSUFFIX
#undef REGISTERSTRINGSSUFFIX
#endif

extern HContainer_t *gDefs;
extern void InitGDefs();

#define DRMSSTRING(X, Y) hcon_insert(gDefs, #X, #Y);
#define REGISTERSTRINGSPREFIX static void defs_init() { InitGDefs();
#define REGISTERSTRINGSSUFFIX }

void defs_term();
const char *defs_getval(const char *key);
const char *drms_build_root(void);

#endif /* _DEFS_H */
