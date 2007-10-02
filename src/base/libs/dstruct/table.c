#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "table.h"
#include "xassert.h"
#include "xmem.h"

#define TRUE 1
#define FALSE 0

void table_init(int maxsize, Table_t *S, 
		int (*not_equal)(const void *, const void *))
{
  S->not_equal = not_equal;
  if (maxsize>0)
  {
    XASSERT((S->data = (Entry_t *) malloc((size_t) maxsize * sizeof(Entry_t))));
  }
  else
    S->data = NULL;

  S->size = 0;
  S->maxsize = maxsize;
}


void table_copy(Table_t *dst, Table_t *src)
{
  table_init(src->maxsize, dst, src->not_equal);
  dst->size = src->size;  
  memcpy(dst->data, src->data, src->size*sizeof(Entry_t));
}



void table_free(Table_t *S)
{
  if (S->data)
  {
    free(S->data);
    S->data=NULL;
  }
}

void table_insert(Table_t *S, const void *key, const void *value)
{
  int i;
  Entry_t *tmp;

  for(i=0; i<S->size; i++) 
    if ( !(*S->not_equal)(key,S->data[i].key) ) 
      break;

  if ( i>=S->maxsize ) 
  {
    /* Out of space. Double table size. */
    tmp = S->data;
    XASSERT((S->data = malloc(2*(S->maxsize+1)*sizeof(Entry_t))));
    if (tmp)
    {
      memcpy(S->data, tmp, S->maxsize*sizeof(Entry_t));
      free(tmp);
    }
    S->maxsize = 2*(S->maxsize+1);
    ++S->size;
  }
  else
  {
    /* We did not run out of space. Check if we are overwriting an existing
       entry, in which case we should not increment the size counter. */
    if ( i>=S->size )
      ++S->size;
  }
  S->data[i].key = key;
  S->data[i].value = value;
}

int table_remove(Table_t *S, const void *key)
{
  int i;
  
  for(i=0; i < S->size; i++) 
  {
    if ( !(*S->not_equal)(key,S->data[i].key) ) 
    {
      --S->size;
      S->data[i].value = S->data[S->size].value;
      S->data[i].key = S->data[S->size].key;
      return 0; 
    }
  }
  return 1;
}

const void *table_lookup(Table_t *S, const void *key)
{
  int i;

  for(i=0; i < S->size; i++) 
  {
    if ( !(*S->not_equal)(key,S->data[i].key) )
      return S->data[i].value;
  }
  return NULL;
}


void table_map(Table_t *S, void (*f)(const void *key, const void *value))
{
  int i;

  for(i=0; i < S->size; i++) 
    f(S->data[i].key, S->data[i].value);
}


int table_member(Table_t *S, const void *key)
{
  int i;

  for(i=0; i < S->size; i++) 
  {
    if ( !(*S->not_equal)(key,S->data[i].key) )
      return TRUE;
  }
  return FALSE;
}

int table_len(Table_t *S)
{
  return S->size;
}

