#ifndef _TABLE_H
#define _TABLE_H

#include "jsoc.h"

typedef struct Entry_struct {
  const void *key;
  const void *value;
} Entry_t;

typedef struct Table_struct {
  int (*not_equal)(const void *, const void *);
  int size;
  int maxsize;
  Entry_t *data;
} Table_t;

void table_free(Table_t *S);
void table_init(int maxsize, Table_t *S, int (*not_equal)(const void *, const void *));
void table_copy(Table_t *dst, Table_t *src);
void table_insert(Table_t *S, const void *key, const void *value);
int table_remove(Table_t *S, const void *key);
void table_map(Table_t *S, void (*f)(const void *, const void *));
void table_map_data(Table_t *S, void (*f)(const void *key, const void *value, const void *data), const void *data);
int table_member(Table_t *S, const void *key);
const void *table_lookup(Table_t *S, const void *key);
int table_len(Table_t *S);

#endif

