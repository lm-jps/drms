#include <stdio.h>
#include <assert.h>
#include "hcontainer.h"
#include "hash_table.h"


#define NUM_INSERT (1000)
#define NUM_DELETE (NUM_INSERT/2)

typedef struct test_s
{
  int x;
  double y;
  float *z;
} test_t;


void deep_copy(const void *dst1, const void *src1)
{
  test_t *dst, *src;
  dst = (test_t *) dst1;
  src = (test_t *) src1;
  
  *dst = *src;
  dst->z = (float *)malloc(10*sizeof(float));
  memcpy(dst->z, src->z, 10*sizeof(float));
}

void deep_free(const void *ptr1)
{
  test_t *ptr;
  ptr = (test_t *)ptr1;
  free(ptr->z);
}

void print(test_t *ptr)
{
  int i;
  if (ptr)
  {
    printf("x = %d, y = %lf, z = [", ptr->x, ptr->y);
    for (i=0; i<10; i++)
      printf(" %f",ptr->z[i]);
    printf("]\n");
  }
  else  
    printf("EMPTY\n");
}
  
main()
{
  int i,j, count;
  HContainer_t hc, hc1;
  test_t *ptr, *ptr2;
  char key[10];
  HIterator_t hit;

  /* Initialize the HContainer. */
  hcon_init(&hc, sizeof(test_t), 12, deep_free, deep_copy);

  /* Insert data. */
  for (j=0; j<NUM_INSERT; j++)
  {
    sprintf(key,"key%06d",j);
    ptr = hcon_allocslot(&hc, key);
    ptr->x = (int) j;
    ptr->y = (double) j;
    ptr->z = (float *)malloc(10*sizeof(float));
    for (i=0; i<10; i++)
      ptr->z[i] = (float)i;

    /* Look it up immediately to make sure it got inserted right. */
    ptr2 = hcon_lookup(&hc, key);
    assert(ptr2==ptr); /* garbage in should equal garbage out... */
  }

  /* Test lookup. */
  sprintf(key,"key%06d",NUM_INSERT/2);
  ptr = hcon_lookup(&hc, key);
  print(ptr);


  /* Test copy of complete container. */
  hcon_copy(&hc1, &hc);
  ptr = hcon_lookup(&hc1, key);
  print(ptr);

  /* Test removing a single element */
  hcon_remove(&hc1, key);

  /* Make sure it's gone. */
  ptr = hcon_lookup(&hc1, key);
  print(ptr);


  /* Make sure it's still in the other structure and that we
     do not have any crossing pointers! */
  ptr = hcon_lookup(&hc, key);
  print(ptr);

  /* Print stats. */
  hcon_stat(&hc1);

  /* Free first structure. */
  hcon_free(&hc);


  /* Remove somw elements at random. */
  for (i=0; i<NUM_DELETE; i++)
  {
    sprintf(key,"key%06d",rand()%NUM_INSERT);
    hcon_remove(&hc1,key);
  }

  /* Print stats. */
  hcon_stat(&hc1);


  /* Create an iterator for hc1 and loop over elements. */
  hiter_new(&hit, &hc1);
  count = 0;
  while( (ptr=hiter_getnext(&hit)) != NULL )
  {
    count++;
    print(ptr);
  }
  printf("Count = %d\n",count);

  hcon_free(&hc1);
}




