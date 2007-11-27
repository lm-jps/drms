/*
 *  hcontainer.c						2007.11.26
 *
 *  functions defined:
 *	hcon_init
 *	hcon_allocslot_lower
 *	hcon_allocslot
 *	hcon_index2slot
 *	hcon_lookup_lower
 *	hcon_lookup
 *	hcon_lookup_ext
 *	hcon_lookupindex
 *	hcon_member_lower
 *	hcon_member
 *	hcon_free
 *	hcon_remove
 *	hcon_print
 *	hcon_printf
 *	hcon_map
 *	hcon_copy
 *	hcon_stat
 *	hiter_new
 *	hiter_rewind
 *	hiter_getcurrent
 *	hiter_getnext
 *	hcon_create
 *	hcon_destroy
 *	hcon_insert
 *	hiter_create
 *	hiter_destroy
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "hash_table.h"
#include "hcontainer.h"
#include "util.h"
#include "xassert.h"
#include "xmem.h"

#define TABLESIZE (0) /* Initial number of slots allocated in each hash bin. */
#define HASH_PRIME (23)  /* Number of hash bins. */
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
void hcon_init (HContainer_t *hc, int datasize, int keysize,
    void (*deep_free)(const void *value),
    void (*deep_copy)(const void *dst, const void *src)) {
  HCBuf_t *buf;

  hc->num_total = 0;
  hc->datasize = datasize;
  hc->keysize = keysize;
  hc->deep_free = deep_free;
  hc->deep_copy = deep_copy;
  hash_init(&hc->hash, HASH_PRIME, TABLESIZE,
	    (int (*)(const void *, const void *))strcmp, hash_universal_hash);

  /* Allocate the first buffer structure. */
  XASSERT( hc->buf = malloc(sizeof(HCBuf_t)) );
  buf = hc->buf;
  buf->num_max = HCON_INITSIZE;
  XASSERT( buf->freelist = malloc(buf->num_max*sizeof(char)) );
  XASSERT( buf->data = malloc(buf->num_max*datasize) );
  XASSERT( buf->keys = malloc(buf->num_max*keysize) );
  buf->firstfree = 0;
  buf->firstindex = 0;
  buf->next = NULL;
  memset(buf->freelist, 1, buf->num_max*sizeof(char));
}
/*
 *
 */
void *hcon_allocslot_lower (HContainer_t *hc, const char *key) {
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
void *hcon_allocslot (HContainer_t *hc, const char *key) {
  unsigned long index;
  void *slot;
  char *keyslot;
  HCBuf_t *buf, *oldbuf;

  index = (unsigned long)hash_lookup(&hc->hash, key);
  if (index == 0)
  {
    /* This is an item with a not previously seen key. */
    ++hc->num_total;
    keyslot = NULL;
    slot = NULL;
    oldbuf = NULL;
    buf = hc->buf;
    while (slot == 0 && buf != NULL)
    {
      /* Look for a slot in the current buffer. */
      if (buf->firstfree < buf->num_max)	
      {	
	index = buf->firstindex + buf->firstfree;
	slot = &buf->data[hc->datasize*buf->firstfree];	
	keyslot = &buf->keys[hc->keysize*buf->firstfree];
	strncpy(keyslot,key,hc->keysize);
	keyslot[hc->keysize-1] = 0;
	buf->freelist[buf->firstfree] = 0;
	/* Scan the freelist to find the next free slot. */
	buf->firstfree++;
	while (buf->firstfree<buf->num_max && buf->freelist[buf->firstfree]==0)
	  buf->firstfree++;
      }
      oldbuf = buf;
      buf = buf->next;
    }
    if (slot == 0 && buf==NULL)  /* No more free slots; add another buffer; */
    {
      XASSERT( buf = malloc(sizeof(HCBuf_t)) );
      oldbuf->next = buf;
      buf->num_max = 2*oldbuf->num_max;
      buf->firstindex = oldbuf->firstindex + oldbuf->num_max;
      XASSERT( buf->data = malloc(buf->num_max*hc->datasize) );
      XASSERT( buf->keys = malloc(buf->num_max*hc->keysize) );
      XASSERT( buf->freelist = malloc(buf->num_max) );
      memset(buf->freelist, 1, buf->num_max);
      buf->firstfree = 1;
      buf->freelist[0] = 0;
      buf->next = NULL;
      slot = &buf->data[0];
      keyslot = &buf->keys[0];
      strncpy(keyslot,key,hc->keysize);
      keyslot[hc->keysize-1] = 0;
      index = buf->firstindex;
    }
    /* Insert the index in the hash table. */
    hash_insert(&hc->hash, keyslot, (void *)(index+1));
  }
  else
  {
    index--;   /* Find slot corresponding to index. */
    slot = hcon_index2slot(hc, index, NULL);
  }	  
  return slot;
}
/*
  Convert index to slot pointer. 
*/
void *hcon_index2slot (HContainer_t *hc, int index, HCBuf_t **outbuf) {
  HCBuf_t *buf;  

  buf = hc->buf;
  while(buf && index>=(buf->firstindex+buf->num_max))
    buf = buf->next;  
  if (buf==NULL)
  {
    if (outbuf)
      *outbuf=NULL;
    return NULL;
  }
  index -= buf->firstindex;
  assert(buf->freelist[index] == 0);
  if(outbuf)
    *outbuf=buf;
  return &buf->data[index*hc->datasize];
}
/*
 *
 */
void *hcon_lookup_lower (HContainer_t *hc, const char *key) {
  char *tmp = strdup(key);
  strtolower(tmp);
  void *slot = hcon_lookup(hc, tmp);
  free(tmp);
  return slot;
}
/*
  Returns a pointer to the slot indexed by key.  If no element 
  index by key is present then it return NULL.
 
*/
void *hcon_lookup (HContainer_t *hc, const char *key) {
  unsigned long index;
  index = (unsigned long)hash_lookup(&hc->hash, key);
  if (index == 0)
    return NULL;
  else
    return hcon_index2slot(hc, index-1, NULL);  
}
/*
 *  Same as hcon_lookup, except that it returns the key as well
 */
void *hcon_lookup_ext (HContainer_t *hc, const char *keyin,
    const char **keyout) {
  unsigned long index;
  HCBuf_t *outbuf = NULL;
  void *ret = NULL;

  index = (unsigned long)hash_lookup(&hc->hash, keyin);
  if (index == 0)
    return NULL;
  else
  {
     ret = hcon_index2slot(hc, index - 1, &outbuf);
     *keyout = &((outbuf->keys)[(index - 1 - outbuf->firstindex) * hc->keysize]);
     return ret;
  }
}
/*
 *
 */
void *hcon_lookupindex (HContainer_t *hc, int index) {
   return hcon_index2slot(hc, index, NULL);
}
/*
 *  Returns 1 if an element indexed by key exists in the container.
 *    Returns 0 otherwise.
 */
int hcon_member_lower (HContainer_t *hc, const char *key) {
   int exists = 0;
   char *tmp = strdup(key);
   strtolower(tmp);
   exists = (hash_lookup(&hc->hash, tmp) != 0);
   free(tmp);
   return exists;
}
/*
 *
 */
int hcon_member (HContainer_t *hc, const char *key) {
  return (hash_lookup(&hc->hash, key) != 0);
}
/*
 *  Free container. If "deep_free" is not NULL it is called
 *    with every slot as argument.
 */
void hcon_free (HContainer_t *hc) {
  int i;
  HCBuf_t *buf, *buf0;

  hash_free(&hc->hash);  
  buf = hc->buf;
  while (buf)
  {
    if (hc->deep_free)
    {
      /* If a deep freeing function was provided, then apply it to 
	 all the filled slots in the buffer. */
      for (i=0; i<buf->num_max; i++)
      {
	if (!buf->freelist[i])
	  (*hc->deep_free)(&buf->data[i*hc->datasize]);
      }
    }
    free(buf->data);
    free(buf->keys);
    free(buf->freelist);
    buf0 = buf;
    buf = buf->next;
    free(buf0);
  }      
}
/*
 *  Remove the element indexed by key from the container. 
 *  If "deep_free" is not NULL it is called with the slot.
*/
void hcon_remove (HContainer_t *hc, const char *key) {
  unsigned long index;
  HCBuf_t *buf;
  void *slot;

  index = (unsigned long)hash_lookup(&hc->hash, key);
  if (index != 0)
  {
    index--;
    slot = hcon_index2slot(hc, index, &buf); /* Get the adress and the buffer. */

    if (hc->deep_free)  /* Deep free the object if possible. */
      (*hc->deep_free)(slot);
    
    /* Update freelist and firstfree for the buffer. */
    index -= buf->firstindex;
    buf->freelist[index] = 1;
    if (index < buf->firstfree)
      buf->firstfree = index;
    hash_remove(&hc->hash, key);
    --hc->num_total;
  }      
}
/*
 *
 */
void hcon_print (HContainer_t *hc) {
   const char *key;
   char printbuf[2048];
   void *data = NULL;

   HIterator_t *hit = hiter_create(hc);
   while((data = hiter_extgetnext(hit, &key)) != NULL)
   {
      fprintf(stdout, "%s\n", key);
   }
}
/*
 *
 */
void hcon_printf (FILE *fp, HContainer_t *hc) {
   const char *key;
   char printbuf[2048];
   void *data = NULL;

   HIterator_t *hit = hiter_create(hc);
   while((data = hiter_extgetnext(hit, &key)) != NULL)
   {
      fprintf(fp, "%s\n", key);
   }
}
/*
 *  Apply the function fmap to every element in the container. 
 */
		/* XXX - THIS MAY NOT WORK */
void hcon_map (HContainer_t *hc, void (*fmap)(const void *value)) {
  int i;
  HCBuf_t *buf;  

  buf = hc->buf;
  while(buf)
  {
    for (i=0; i<buf->num_max; i++)
    {
      if (buf->freelist[i]==0)
	fmap((void *)&buf->data[hc->datasize*i]);
    }
    buf = buf->next;
  }
}
/*
 *  Do a deep copy of the entire container. If "deep_copy" is not NULL it is
 *    called for every slot.
 */
void hcon_copy (HContainer_t *dst, HContainer_t *src) {
  int i;
  HCBuf_t *oldbuf, *buf, *sbuf;  


  dst->num_total = src->num_total;
  dst->datasize = src->datasize;
  dst->keysize = src->keysize;
  hash_copy(&dst->hash, &src->hash);
  dst->deep_free = src->deep_free;
  dst->deep_copy = src->deep_copy;

  oldbuf = NULL;
  /* Allocate the first buffer structure. */
  XASSERT( dst->buf = malloc(sizeof(HCBuf_t)) );
  buf = dst->buf;
  sbuf = src->buf;
  while(sbuf)
  {
    if (oldbuf)
      oldbuf->next = buf;

    buf->num_max = sbuf->num_max;
    buf->firstfree = sbuf->firstfree;
    buf->firstindex = sbuf->firstindex;

    XASSERT( buf->freelist = malloc(buf->num_max*sizeof(char)) );
    memcpy(buf->freelist, sbuf->freelist, buf->num_max*sizeof(char));

    XASSERT( buf->keys = malloc(buf->num_max*src->keysize) );
    memcpy(buf->keys, sbuf->keys, buf->num_max*src->keysize);    

    XASSERT( buf->data = malloc(buf->num_max*src->datasize) );
    if (!src->deep_copy) /* Deep copy contents if possible. */
      memcpy(buf->data, sbuf->data, buf->num_max*src->datasize);
    else
      memset(buf->data, 0, buf->num_max*src->datasize);

    for (i=0; i<buf->num_max; i++)
    {
      if (buf->freelist[i]==0)
      {
	if (src->deep_copy) /* Deep copy contents if possible. */
	  (*src->deep_copy)(&buf->data[i*dst->datasize], 
			    &sbuf->data[i*src->datasize]);
	hash_insert(&dst->hash, &buf->keys[i*dst->keysize], 
		    (void *)(buf->firstindex+((unsigned long)i+1)));
      }
    }

    oldbuf = buf;
    buf->next = NULL;
    sbuf = sbuf->next;
    if (sbuf)
      XASSERT( buf = malloc(sizeof(HCBuf_t)) );
  }
}
/*
 *  Print various statistics about the container.
 */
void hcon_stat (HContainer_t *hc) {
  int i, num=1, free, total_free=0, total_slots=0;
  HCBuf_t *buf;

  printf("=========================================================================\n");
  buf = hc->buf;
  while(buf)
  {
    printf("Buffer %d: firstindex           = %7d\n",num,buf->firstindex);
    printf("Buffer %d: firstfree            = %7d\n",num,buf->firstfree);
    for (i=0, free=0; i<buf->num_max; i++)
      free += buf->freelist[i];
    printf("Buffer %d: number of slots      = %7d\n",num,buf->num_max);
    printf("Buffer %d: number of free slots = %7d\n",num,free);
    total_free += free;
    total_slots += buf->num_max;
    buf = buf->next;
    num++;
  }
  printf("Data size                  = %8d\n",hc->datasize);
  printf("Key size                   = %8d\n",hc->keysize);
  printf("Total number of items      = %8d\n",hc->num_total);
  printf("Total number of free slots = %8d\n",total_free);
  printf("Total number of slots      = %8d\n",total_slots);
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
/*
 * Iterator object allows (forwards) looping over contents of HContainer
 */
void hiter_new (HIterator_t *hit, HContainer_t *hc) {
  hit->hc = hc;
  hit->cur_buf = hc->buf;
  hit->cur_idx = -1;
}
/*
 *
 */
void hiter_rewind (HIterator_t *hit) {
  hit->cur_buf = hit->hc->buf;
  hit->cur_idx = -1;
}

#ifdef BLAH
/*
 *
 */
void *hiter_getcurrent (HIterator_t *hit) {
  if (hit->cur_idx==-1 || hit->cur_buf==NULL)
    return NULL;
  else
    return &hit->cur_buf->data[hit->cur_idx*hit->hc->datasize];
}
/*
 *
 */
void *hiter_getnext (HIterator_t *hit) {
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
#endif
/*
 *
 */
HContainer_t *hcon_create (int datasize, int keysize,
    void (*deep_free)(const void *value),
    void (*deep_copy)(const void *dst, const void *src), void **valArr,
    char **nameArr, int valArrSize) {
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
/*
 *
 */
void hcon_destroy (HContainer_t **cont) {
     if (*cont != NULL)
     {
	  hcon_free(*cont);
	  free(*cont);
	  *cont = NULL;
     }
}
/*
 *
 */
int hcon_insert (HContainer_t *hcon, const char *key, const void *value) {
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
/*
 *
 */
HIterator_t *hiter_create (HContainer_t *cont) {
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
/*
 *
 */
void hiter_destroy (HIterator_t **iter) {
     if (*iter != NULL)
     {
	  free(*iter);
	  *iter = NULL;
     }
}
