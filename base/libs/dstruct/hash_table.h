#ifndef _HASH_TABLE_H
#define _HASH_TABLE_H

#include "table.h"
#include "jsoc.h"

typedef struct Hash_Table_struct {
  unsigned int hashprime;
  int (*not_equal)(const void *, const void *);
  unsigned int (*hash)(const void *);
  Table_t *list;
} Hash_Table_t;

void hash_init(Hash_Table_t *h, const unsigned int hashprime, const int initbinsize, 
	       int (*not_equal)(const void *, const void *), 
	       unsigned int (*hash)(const void *));
void hash_free(Hash_Table_t *h);
void hash_copy(Hash_Table_t *dst, Hash_Table_t *src);
void hash_insert(Hash_Table_t *h, const void *key, const void *value );
void hash_remove(Hash_Table_t *h, const void *key); 
int hash_member(Hash_Table_t *h, const void *key);
const void *hash_lookup(Hash_Table_t *h, const void *key);
int hash_size(Hash_Table_t *h);
void hash_stat(Hash_Table_t *h);
void hash_map(Hash_Table_t *h, void (*f)(const void *, const void *));
void hash_map_data(Hash_Table_t *h, void (*f)(const void *, const void *, const void *data), const void *data);
unsigned long long hash_universal_hash(const void *v);
#endif
