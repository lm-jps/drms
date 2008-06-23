#ifndef _DEFS_H
#define _DEFS_H

int defs_register(const char *filepath);
void defs_term();
const char *defs_getval(const char *key);

#endif /* _DEFS_H */
