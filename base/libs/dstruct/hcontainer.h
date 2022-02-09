/**
\file hcontainer.h
*/
#ifndef _HCONTAINER_H
#define _HCONTAINER_H

#include "hash_table.h"
#include "jsoc.h"

/* Initial size in number of elements. */
#define HCON_INITSIZE 2

/** @brief HContainerElement struct */
struct HContainerElement_struct
{
  char *key;
  void *val;
};

typedef struct HContainerElement_struct HContainerElement_t;

/** \brief HContainer struct */
struct HContainer_struct {
  int num_total;          /* Number of items in container. */
  int datasize;           /* Size of data items in bytes. */
  int keysize;            /* Max size of key string in bytes. */
  Hash_Table_t hash;      /* Hash table pointing into buffer. */
  void (*deep_free)(const void *value);               /* Function for deep freeing items. */
  void (*deep_copy)(const void *dst, const void *src); /* Function for deep copy. */
};

/** \brief HContainer struct reference */
typedef struct HContainer_struct HContainer_t;

typedef struct HIterator_struct {
  HContainer_t *hc;
  int curr;                    /* index of current element in elems */
  HContainerElement_t **elems; /* array of all hcontainer elements; assigned when
                                * iterator is created */
  int nelems;                  /* number of elements in elems (number of used elem slots in array) */
  int szelems;                 /* number of elem slots allocated in elems */
} HIterator_t;

struct Bundle_struct
{
  void (*fmap)(const void *value, void *data);
  void *data;
};

typedef struct Bundle_struct HContainerBundle_t;

void hcon_init(HContainer_t *hc, int datasize, int keysize,
	       void (*deep_free)(const void *value),
	       void (*deep_copy)(const void *dst, const void *src));
void hcon_init_ext(HContainer_t *hc, unsigned int hashprime, int datasize, int keysize,
                   void (*deep_free)(const void *value),
                   void (*deep_copy)(const void *dst, const void *src));

/* allows the caller to specify an initial number of bins too */
void hcon_init_ext2(HContainer_t *hc, unsigned int hashprime, unsigned int initial_bin_size, int datasize, int keysize, void (*deep_free)(const void *value), void (*deep_copy)(const void *dst, const void *src));
void *hcon_allocslot_lower(HContainer_t *hc, const char *key);
void *hcon_allocslot(HContainer_t *hc, const char *key);
void *hcon_lookup_lower(HContainer_t *hc, const char *key);
void *hcon_lookup(HContainer_t *hc, const char *key);
void *hcon_lookup_ext(HContainer_t *hc, const char *keyin, const char **keyout);
void *hcon_getn(HContainer_t *hcon, unsigned int n);
int hcon_member_lower(HContainer_t *hc, const char *key);
int hcon_member(HContainer_t *hc, const char *key);
void hcon_free(HContainer_t *hc);
void hcon_remove(HContainer_t *hc, const char *key);
void hcon_print(HContainer_t *hc);
void hcon_printf(FILE *fp, HContainer_t *hc);
void hcon_map(HContainer_t *hc, void (*fmap)(const void *value));
void hcon_map_ext(HContainer_t *hc, void (*fmap)(const void *value, void *data), void *data);
void hcon_copy(HContainer_t *dst, HContainer_t *src);
void hcon_copy_to_initialized(HContainer_t *dst, HContainer_t *src);
//int hcon_size(HContainer_t *hc);
void hcon_stat(HContainer_t *hc);


void hiter_new(HIterator_t *hit, HContainer_t *hc);
void hiter_new_sort(HIterator_t *hit, HContainer_t *hc, int (*comp)(const void *, const void *));
void hiter_free(HIterator_t *hit);
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

/* Iterator object allows (forwards) looping over contents of
   HContainer. */
static inline void *hiter_getcurrent(HIterator_t *hit)
{
   if (hit->curr == -1)
     return NULL;
   else
     return (hit->elems[hit->curr])->val;
}

/* This just traverses all the buffers, looking for the next non-free slot. */
static inline void *hiter_getnext(HIterator_t *hit)
{
   void *value = NULL;

   if (hit->curr + 1 < hit->hc->num_total)
   {
      hit->curr++;
      value = (hit->elems[hit->curr])->val;
   }

   return value;
}

static inline void *hiter_extgetnext(HIterator_t *hit, const char **key)
{
   void *value = NULL;

   if (hit->curr + 1 < hit->hc->num_total)
   {
      hit->curr++;
      value = (hit->elems[hit->curr])->val;
      if (key)
      {
         *key = (hit->elems[hit->curr])->key;
      }
   }

   return value;

}

static inline void *hcon_getval(HContainerElement_t *elem)
{
   if (elem)
   {
      return elem->val;
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
