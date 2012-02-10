#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "hash_table.h"
#include "hcontainer.h"
#include "util.h"
#include "xassert.h"
#include "xmem.h"
#include "timer.h"

#define TABLESIZE (0) /* Initial number of slots allocated in each hash bin. */
#define HASH_PRIME (47)  /* Number of hash bins. */

/*
  Initialize the container. 

  "datasize" is the size of the data slots in the container.
  "keysize" is the maxsize of the key string.
  "deep_free" if an optional function to free fields inside the data
  slots if they contain structs with malloc'ed pointer
  fields. Set to NULL to skip deep freeing.
  "deep_copy" is an optional function to copy fields inside the data
  slots. Set to NULL to skip deep copying.
*/
void hcon_init(HContainer_t *hc, int datasize, int keysize,
	       void (*deep_free)(const void *value),
	       void (*deep_copy)(const void *dst, const void *src))
{
  hc->num_total = 0;
  hc->datasize = datasize;
  hc->keysize = keysize;
  hc->deep_free = deep_free;
  hc->deep_copy = deep_copy;
  hash_init(&hc->hash, HASH_PRIME, TABLESIZE,
	    (int (*)(const void *, const void *))strcmp, hash_universal_hash);
}

void hcon_init_ext(HContainer_t *hc, unsigned int hashprime, int datasize, int keysize,
                   void (*deep_free)(const void *value),
                   void (*deep_copy)(const void *dst, const void *src))
{
  hc->num_total = 0;
  hc->datasize = datasize;
  hc->keysize = keysize;
  hc->deep_free = deep_free;
  hc->deep_copy = deep_copy;
  hash_init(&hc->hash, hashprime, TABLESIZE,
	    (int (*)(const void *, const void *))strcmp, hash_universal_hash);
}

void *hcon_allocslot_lower(HContainer_t *hc, const char *key)
{
  char *tmp = strdup(key);
  strtolower(tmp);
  void *slot = hcon_allocslot(hc, tmp);
  free(tmp);
  return slot;
}

/*
  Allocate a new slot indexed by the string "key". Returns a void
  pointer to data slot of size "datasize". If an element indexed
  by key already exists this functions returns the same as hcon_lookup. 
*/

/* Return the value field inside the hcontainer element. This field is necessarily a pointer
 * (e.g., it may point to an int, or it may point to a char *). */
void *hcon_allocslot(HContainer_t *hc, const char *key)
{
  void *slot = NULL;
  HContainerElement_t *elem = NULL; /* Pointer to allocated elem struct */

  elem = (HContainerElement_t *)hash_lookup(&hc->hash, key);
  
  if (elem == NULL)
  {
    /* This is an item with a previously unseen key. */
     elem = malloc(sizeof(HContainerElement_t));

     if (elem)
     {
        memset(elem, 0, sizeof(HContainerElement_t));
        elem->key = strdup(key); /* ignore keysize - left over from previous implementation */
        elem->val = malloc(hc->datasize);
        memset(elem->val, 0, hc->datasize);

        /* The pointers elem->key and elem are copied directly into the hash table.  
         * Don't free key in hconfreemap, since it is equivalent to elem->key, and hconfreemap
         * frees elem->key and elem->val (if key is freed, this will double free elem->key.
         * hconfreemap also needs to free elem since it, as well as elem->key and elem->val
         * all point to memory allocated in this function */

        hash_insert(&hc->hash, elem->key, (void *)(elem));
        ++hc->num_total;
     }
  }

  if (elem)
  {
     slot = elem->val;
  }

  return slot;
}

void *hcon_lookup_lower(HContainer_t *hc, const char *key)
{
  char *tmp = strdup(key);
  strtolower(tmp);
  void *slot = hcon_lookup(hc, tmp);
  free(tmp);
  return slot;
}

/*
  Returns the value field in the HContainerElement_t stored in the underlying hash table, unless
  no such key exists in the hash table, in which case a NULL is returned. 
*/
void *hcon_lookup(HContainer_t *hc, const char *key)
{
   HContainerElement_t *elem = NULL; /* Pointer to allocated elem struct */

   elem = (HContainerElement_t *)hash_lookup(&hc->hash, key);
   if (elem == NULL)
    return NULL;
  else
    return elem->val;
}

/* Same as hcon_lookup, except that it returns the key as well */
void *hcon_lookup_ext(HContainer_t *hc, const char *keyin, const char **keyout)
{
   HContainerElement_t *elem = NULL; /* Pointer to allocated elem struct */
   void *ret = NULL;

   elem = (HContainerElement_t *)hash_lookup(&hc->hash, keyin);
   if (elem == NULL)
     return NULL;
   else
   {
      ret = elem->val;
      *keyout = elem->key;
      return ret;
   }
}

void *hcon_getn(HContainer_t *hcon, unsigned int n)
{
   HContainerElement_t *elem = NULL; /* Pointer to allocated elem struct */
   HIterator_t *hit = hiter_create(hcon);

   if (hit && n < hit->nelems)
   {
      elem = hit->elems[n];
      hiter_destroy(&hit);
   }
   
   if (elem)
   {
      return elem->val;
   }
   else
   {
      return NULL;
   }
}

/*
  Returns 1 if an element indexed by key exists in the container.
  Returns 0 otherwise.
*/
int hcon_member_lower(HContainer_t *hc, const char *key)
{
   int exists = 0;
   char *tmp = strdup(key);
   strtolower(tmp);
   exists = (hash_lookup(&hc->hash, tmp) != NULL);
   free(tmp);
   return exists;
}

int hcon_member(HContainer_t *hc, const char *key)
{
  return (hash_lookup(&hc->hash, key) != NULL);
}

static void hconfreemap(const void *key, const void *value, const void *data)
{
   HContainer_t *hcon = (HContainer_t *)data;
   HContainerElement_t *elem = NULL;

   /* Don't free key - the memory it allocates (if any) wasn't allocated by 
    * hcon code. */

   if (value)
   {
      elem = (HContainerElement_t *)value;
      XASSERT(elem && elem->val);

      if (hcon->deep_free && elem->val)
      {
         (*hcon->deep_free)(elem->val);
      }

      /* Need to deep-free key and val */
      if (elem->key)
      {
         free(elem->key);
      }

      if (elem->val)
      {
         free(elem->val);
      }

      /* Free the hcon elem itself. */
      free((void *)value);
   }
}

/* Free container. If "deep_free" is not NULL it is applied to every value in the container. */
void hcon_free(HContainer_t *hc)
{
  /* Apply a function that will free the keys and value (and also deep-free the values, if a deep-free
   * function was provided). After this call, the hash table will contain garbage for keys and values. */
  hash_map_data(&hc->hash, hconfreemap, hc);

  hc->num_total = 0;
  hc->datasize = 0;
  hc->keysize = 0;
  hc->deep_free = NULL;
  hc->deep_copy = NULL;

  /* Free hash table - this frees an array of key-value structures; the actual key and value fields
   * are freed by hconfreemap. */
  hash_free(&hc->hash);  
}

/*
  Remove the element indexed by key from the container. 
  If "deep_free" is not NULL it is called with the slot.
*/
void hcon_remove(HContainer_t *hc, const char *key)
{
   HContainerElement_t *elem = NULL; /* Pointer to allocated elem struct */

   elem = (HContainerElement_t *)hash_lookup(&hc->hash, key);
   if (elem != NULL)
   {
      /* Must remove from hash first - elem->key is the value of the key in the hash, 
       * so you can't delete it, then call hash_remove.  If you do, you won't delete
       * the key-value pair from the hash table because you deleted the key so 
       * the hash look up will fail. */
      
      /* Remove the key-value entries from the underlying hash table - does not free key or value. */
      hash_remove(&hc->hash, key);

      if (elem->key)
      {
         free(elem->key);
         elem->key = NULL;
      }

      if (elem->val)
      {
         if (hc->deep_free)  /* Deep free the object if possible. */
         {
            (*hc->deep_free)(elem->val);
         }

         free(elem->val);
         elem->val = NULL;
      }
    
      free(elem);
     
      --hc->num_total;
   }      
}

void hcon_print(HContainer_t *hc)
{
   const char *key;
   void *data = NULL;

   HIterator_t *hit = hiter_create(hc);
   while((data = hiter_extgetnext(hit, &key)) != NULL)
   {
      fprintf(stdout, "%s\n", key);
   }
}

void hcon_printf(FILE *fp, HContainer_t *hc)
{
   const char *key;
   void *data = NULL;

   HIterator_t *hit = hiter_create(hc);
   while((data = hiter_extgetnext(hit, &key)) != NULL)
   {
      fprintf(fp, "%s\n", key);
   }
}

/*
  Apply the function fmap to every element in the container. 
*/
static void hconmapmap(const void *key, const void *value, const void *data)
{
   void (*fmap)(const void *value) = (void (*)(const void *value))data;

   if (value)
   {
      (*fmap)(((HContainerElement_t *)value)->val); /* Apply fmap to every value in the hcontainer element (which is value). */
   }
}

static void hconmapmap_ext(const void *key, const void *value, const void *bundle)
{
   HContainerBundle_t *bundlearg = (HContainerBundle_t *)bundle;
   void (*fmap)(const void *value, void *data) = bundlearg->fmap;
   void *data = bundlearg->data;

   /* hash value */
   if (value)
   {
      (*fmap)(((HContainerElement_t *)value)->val, data); /* Apply fmap to every value in the hcontainer element (which is value);
                                                           * this version of the function also takes an additional argument 
                                                           * to be used for virtually unlimited purposes. */
   }
}

void hcon_map(HContainer_t *hc, void (*fmap)(const void *value))
{
   hash_map_data(&hc->hash, hconmapmap, fmap);
}

void hcon_map_ext(HContainer_t *hc, void (*fmap)(const void *value, void *data), void *data)
{
   HContainerBundle_t bundle;

   bundle.fmap = fmap;
   bundle.data = data;

   hash_map_data(&hc->hash, hconmapmap_ext, &bundle);
}

/*
  Do a deep copy of the entire container. If "deep_copy" is not NULL it is applied to every hcontainer-element value.
*/
static void hconcopymap(const void *key, const void *value, const void *data)
{
   HContainer_t *dst = (HContainer_t *)(data);
   HContainerElement_t *elem = (HContainerElement_t *)value;

   /* Can't use hash_copy() since in new implementation, the hash table stores pointers
    * to hcontainer elements (not indices). Instead, just insert a copy of each item into
    * the dst hcontainer. */
 
   /* Should do deep copy, if deep_copy was provided. */
   hcon_insert(dst, key, elem->val);
}

void hcon_copy(HContainer_t *dst, HContainer_t *src)
{
   hcon_init(dst, src->datasize, src->keysize, src->deep_free, src->deep_copy);

   /* Apply a function that inserts into the dst hcontainer a key-value pair from the src hcontainer. This will
    * deep-copy if src->deep_free != NULL. */
   hash_map_data(&src->hash, hconcopymap, dst);
}

/*
  Print various statistics about the container.
*/
void hcon_stat(HContainer_t *hc)
{
  printf("=========================================================================\n");
  printf("Data size                  = %8d\n",hc->datasize);
  printf("Key size                   = %8d\n",hc->keysize);
  printf("Total number of items      = %8d\n",hc->num_total);
  printf("=========================================================================\n");
}

/*
  Print the total number of elements in the container.
*/
/*int hcon_size(HContainer_t *hc)
{
  return hc->num_total;
}
*/

/* Iterator object allows (forwards) looping over contents of
   HContainer. */

static void hconelemmap(const void *key, const void *value, const void *data)
{
   HIterator_t *hit = (HIterator_t *)(data);

   /* May need to resize array. */
   if (hit->nelems + 1 > hit->szelems)
   {
      /* expand argarr buffer */
      int origsz = hit->szelems;

      hit->szelems *= 2;
      hit->elems = realloc(hit->elems, hit->szelems * sizeof(HContainerElement_t *));
      memset(&hit->elems[origsz], 0, (hit->szelems - origsz) * sizeof(HContainerElement_t *));
   }

   hit->elems[hit->nelems++] = (HContainerElement_t *)value;
}

void hiter_new(HIterator_t *hit, HContainer_t *hc)
{
  hit->hc = hc;
  hit->curr = -1;
  hit->nelems = 0;
  hit->szelems = 16;
  hit->elems = (HContainerElement_t **)malloc(hit->szelems * sizeof(HContainerElement_t *));
  memset(hit->elems, 0, hit->szelems * sizeof(HContainerElement_t *));

  /* Need to assign the hcontainer elements to hit->elems. */
  hash_map_data(&hit->hc->hash, hconelemmap, hit);
}

void hiter_free(HIterator_t *hit)
{
   if (hit && hit->elems)
   {
      free(hit->elems);
   }
}

void hiter_new_sort(HIterator_t *hit, HContainer_t *hc, int (*comp)(const void *, const void *))
{
   hiter_new(hit, hc);

   /* Now sort */
   qsort(hit->elems, hit->nelems, sizeof(HContainerElement_t *), comp); 
}

void hiter_rewind(HIterator_t *hit)
{
  hit->curr = -1;
}

HContainer_t *hcon_create(int datasize, 
			  int keysize,
			  void (*deep_free)(const void *value),
			  void (*deep_copy)(const void *dst, const void *src),
			  void **valArr,
			  char **nameArr,
			  int valArrSize)
{
   HContainer_t *cont = (HContainer_t *)malloc(sizeof(HContainer_t));
   if (cont != NULL)
   {
      hcon_init(cont, datasize, keysize, deep_free, deep_copy);
      int iArr = 0;
	  
      while (valArr && nameArr && iArr < valArrSize)
      {
         void *slot = hcon_allocslot(cont, nameArr[iArr]);
         if (slot != NULL)
         {
            void *src = valArr[iArr];
            memcpy(slot, src, datasize);

            if (deep_copy != NULL)
            {
               (*deep_copy)(slot, src);
            }
         }

         iArr++;
      }
   }

   return cont;
}

void hcon_destroy(HContainer_t **cont)
{
   if (*cont != NULL)
   {
      hcon_free(*cont);
      free(*cont);
      *cont = NULL;
   }
}

int hcon_insert_lower(HContainer_t *hcon, const char *key, const void *value)
{
   char *tmp = strdup(key);
   strtolower(tmp);
   int ret = hcon_insert(hcon, tmp, value);
   free(tmp);
   return ret;
}

int hcon_insert(HContainer_t *hcon, const char *key, const void *value)
{
   int err = 1;

   if (hcon)
   {
      if (!hcon_lookup(hcon, key))
      {
	 void *slot = hcon_allocslot(hcon, key);
	 if (slot)
	 {
	    memcpy(slot, value, hcon->datasize);
	    if (hcon->deep_copy != NULL)
	    {
	       (*(hcon->deep_copy))(slot, value);
	    }

	    err = 0;
	 }
      }
   }

   return err;
}

HIterator_t *hiter_create(HContainer_t *cont)
{
     HIterator_t *iter = NULL;

     if (cont != NULL)
     {
	iter = (HIterator_t *)malloc(sizeof(HIterator_t));
	if (iter != NULL)
	{
	   hiter_new(iter, cont);
	}
     }

     return iter;
}

void hiter_destroy(HIterator_t **iter)
{
   if (*iter != NULL)
   {
      /* Free elems array, but not the hcontainer elements the elems elements point to. */
      if ((*iter)->elems)
      {
         free((*iter)->elems);
      }

      free(*iter);
      *iter = NULL;
   }
}
