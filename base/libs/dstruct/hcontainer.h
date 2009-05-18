/**
\file hcontainer.h
*/
#ifndef _HCONTAINER_H
#define _HCONTAINER_H

#include "hash_table.h"
#include "jsoc.h"

/* Initial size in number of elements. */
#define HCON_INITSIZE 2

typedef struct HCBuf_struct {
  int num_max; /* Number of items currently and max in the buffer. */
  int firstfree;
  int firstindex;
  char *data;           /* buffer holding data items. */
  char *keys;           /* buffer holding key values. */
  char *freelist;       /* Bitmap of free slots in the buffer. */
  struct HCBuf_struct *next; /* Next buffer. */
} HCBuf_t;

/** \brief HContainer struct */
struct HContainer_struct {
  int num_total;          /* Number of items in container. */
  int datasize;           /* Size of data items in bytes. */
  int keysize;            /* Max size of key string in bytes. */
  Hash_Table_t hash;      /* Hash table pointing into buffer. */
  void (*deep_free)(const void *value);               /* Function for deep freeing items. */
  void (*deep_copy)(const void *dst, const void *src); /* Function for deep copy. */
  HCBuf_t *buf;
}; 

/** \brief HContainer struct reference */
typedef struct HContainer_struct HContainer_t;

typedef struct HIterator_struct {
  struct HContainer_struct *hc;
  struct HCBuf_struct *cur_buf;
  int cur_idx;
} HIterator_t;

void hcon_init(HContainer_t *hc, int datasize, int keysize,
	       void (*deep_free)(const void *value),
	       void (*deep_copy)(const void *dst, const void *src));
void *hcon_allocslot_lower(HContainer_t *hc, const char *key);
void *hcon_allocslot(HContainer_t *hc, const char *key);
void *hcon_index2slot(HContainer_t *hc, int index, HCBuf_t **outbuf);
void *hcon_lookup_lower(HContainer_t *hc, const char *key);
void *hcon_lookup(HContainer_t *hc, const char *key);
void *hcon_lookup_ext(HContainer_t *hc, const char *keyin, const char **keyout);
void *hcon_lookupindex(HContainer_t *hc, int index);
int hcon_member_lower(HContainer_t *hc, const char *key);
int hcon_member(HContainer_t *hc, const char *key);
void hcon_free(HContainer_t *hc);
void hcon_remove(HContainer_t *hc, const char *key);
void hcon_print(HContainer_t *hc);
void hcon_printf(FILE *fp, HContainer_t *hc);
void hcon_map(HContainer_t *hc, void (*fmap)(const void *value));
void hcon_copy(HContainer_t *dst, HContainer_t *src);
//int hcon_size(HContainer_t *hc);
void hcon_stat(HContainer_t *hc);


void hiter_new(HIterator_t *hit, HContainer_t *hc);
void hiter_rewind(HIterator_t *hit);
//void *hiter_getcurrent(HIterator_t *hit);
//void *hiter_getnext(HIterator_t *hit);

/*
  Print the total number of elements in the container.
*/
static inline int hcon_size(HContainer_t *hc)
{
  return hc->num_total;
}

static inline int hcon_initialized(HContainer_t *hc)
{
   return (hc->buf != NULL);
}

/* Iterator object allows (forwards) looping over contents of
   HContainer. */

static inline void *hiter_getcurrent(HIterator_t *hit)
{
  if (hit->cur_idx==-1 || hit->cur_buf==NULL)
    return NULL;
  else
    return &hit->cur_buf->data[hit->cur_idx*hit->hc->datasize];
}

static inline void *hiter_getnext(HIterator_t *hit)
{
  int idx;
  HCBuf_t *buf;
  
  buf = hit->cur_buf;
  idx = hit->cur_idx+1;
  
  while (buf)
  {
    while (idx<buf->num_max && buf->freelist[idx]==1)
      idx++;
    if (idx>=buf->num_max)
    {
      buf = buf->next;
      idx = 0;
    }
    else
    {
      hit->cur_buf = buf;
      hit->cur_idx = idx;
      return &buf->data[idx*hit->hc->datasize];
    }
  }
  return NULL;
}

static inline void *hiter_extgetnext(HIterator_t *hit, const char **key)
{
  int idx;
  HCBuf_t *buf;
  
  buf = hit->cur_buf;
  idx = hit->cur_idx+1;
  
  while (buf)
  {
    while (idx<buf->num_max && buf->freelist[idx]==1)
      idx++;
    if (idx>=buf->num_max)
    {
      buf = buf->next;
      idx = 0;
    }
    else
    {
      hit->cur_buf = buf;
      hit->cur_idx = idx;
      if (key)
      {
	 *key = &(buf->keys[idx * hit->hc->keysize]);
      }
      return &buf->data[idx*hit->hc->datasize];
    }
  }
  return NULL;
}

/* Wrappers to make it easy to create containers and iterate through them. */
HContainer_t *hcon_create(int datasize, 
			  int keysize,
			  void (*deep_free)(const void *value),
			  void (*deep_copy)(const void *dst, const void *src),
			  void **valArr,
			  char **nameArr,
			  int valArrSize);
void hcon_destroy(HContainer_t **cont);
int hcon_insert(HContainer_t *hcon, const char *key, const void *value);
int hcon_insert_lower(HContainer_t *hcon, const char *key, const void *value);
HIterator_t *hiter_create(HContainer_t *cont);
void hiter_destroy(HIterator_t **iter);


#endif
