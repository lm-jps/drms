/*
 *  hash_table.c						2007.11.26
 *
 *  functions defined:
 *	hash_universal_hash
 *	hash_init
 *	hash_copy
 *	hash_free
 *	hash_insert
 *	hash_remove
 *	hash_member
 *	hash_lookup
 *	hash_size
 *	hash_stat
 *	hash_map
 */
#include <stdio.h>
#include <stdlib.h>
#include "hash_table.h"
#include "table.h"
#include "xassert.h"
#include "xmem.h"
/*
 *
 */
unsigned int hash_universal_hash (const void *v) {
  char *c = (char *) v;
  unsigned int sum = 0;

  while (*c) sum += ((unsigned int)*c++) + 31 * sum;
  return sum;
}
/*
 *
 */
void hash_init (Hash_Table_t *h, const unsigned int hashprime,
    const int initbinsize, int (*not_equal)(const void *, const void *),
    unsigned int (*hash)(const void *)) {
  unsigned int i;
  
  h->hashprime = hashprime;
  h->hash = hash;
  h->not_equal = not_equal;
  XASSERT(h->list = (Table_t *)malloc (hashprime * sizeof (Table_t)));
  for(i=0; i<hashprime; i++)  
    table_init (initbinsize, &(h->list[i]), not_equal);
}
/*
 *  Deep copy of hash table
 */
void hash_copy (Hash_Table_t *dst, Hash_Table_t *src) {
  unsigned int i;

  dst->hashprime = src->hashprime;
  dst->not_equal = src->not_equal;
  dst->hash = src->hash;
  XASSERT(dst->list = (Table_t *)malloc (dst->hashprime*sizeof (Table_t)));
  for(i=0; i<dst->hashprime; i++) 
    table_copy (&(dst->list[i]), &(src->list[i]));    
}
/*
 *
 */
void hash_free (Hash_Table_t *h) {
  unsigned int i;
  if (h->list) {
    for (i=0; i<h->hashprime; i++) 
      table_free (&(h->list[i]));
    free (h->list);
    h->list = NULL;
  }
}
/*
 *
 */
void hash_insert (Hash_Table_t *h, const void *key, const void *contents) {
  table_insert (&(h->list[h->hash(key) % h->hashprime]), key, contents);
}
/*
 *
 */
void hash_remove (Hash_Table_t *h, const void *key) {
  table_remove (&(h->list[h->hash(key) % h->hashprime]),key);
}
/*
 *
 */
int hash_member (Hash_Table_t *h, const void *key) {
  return table_member (&(h->list[h->hash(key) % h->hashprime]), key);
}
/*
 *
 */
const void *hash_lookup (Hash_Table_t *h, const void *key) {
  return table_lookup (&(h->list[h->hash(key) % h->hashprime]),key);
}
/*
 *
 */
int hash_size (Hash_Table_t *h)
{
  unsigned int i,size;
  for (i=0,size=0; i<h->hashprime; i++)
    size += table_len (&(h->list[i]));
  return size;
}
/*
 *
 */
void hash_stat (Hash_Table_t *h) {
  unsigned int i, len;
  for (i=0; i<h->hashprime; i++) {
    len = table_len (&(h->list[i]));
    if ( len > 0) printf ("%4d: %4d\n",i,len);
  }
}
/*
 *
 */
void hash_map (Hash_Table_t *h, void (*f)(const void *, const void *)) {
  unsigned int i, len;
  for (i=0; i<h->hashprime; i++) {
    len = table_len (&(h->list[i]));
    if (len > 0) table_map (&(h->list[i]),f);
  }
}

