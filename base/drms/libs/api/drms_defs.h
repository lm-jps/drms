#ifndef _DRMS_DEFS_H
#define _DRMS_DEFS_H

#define DEFS_MKPATH(X) CDIR##X

int drms_defs_register(const char *filepath);
void drms_defs_term();
const char *drms_defs_getval(const char *key);

#endif /* _DRMS_DEFS_H */
