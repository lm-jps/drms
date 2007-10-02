#ifndef __PRINTK_H
#define __PRINTK_H

extern int (*printk)(const char *fmt, ...);
extern int (*printkerr)(const char *fmt, ...);

extern void printk_set(int (*std)(const char *fmt, ...),
		       int (*err)(const char *fmt, ...));

#endif
