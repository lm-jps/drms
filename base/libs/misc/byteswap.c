#include "byteswap.h"
#include "xmem.h"

#define SWAP(a,b) {char tmp; tmp = a; a = b; b = tmp;}


void byteswap(int size, int n, char *val)
{
  int i,j;
  char *p;

  if (size==1)
    return;
  
  p = val;
  switch(size)
  {
  case 2:
    for (i=0; i<n; i++)
    {
      SWAP(*p, *(p+1));
      p+=2;
    }
    break;
  case 4:
    for (i=0; i<n; i++)
    {
      SWAP(*p, *(p+3));
      SWAP(*(p+1), *(p+2));
      p+=4;
    }
    break;
  case 8:
    for (i=0; i<n; i++)
    {
      SWAP(*p, *(p+7));
      SWAP(*(p+1), *(p+6));
      SWAP(*(p+2), *(p+5));
      SWAP(*(p+3), *(p+4));
      p+=8;
    }
    break;
  default:
    for (j=0;j<n;j++)
    {
      for(i=0;i<(size/2);i++) 
	SWAP(*(p+i), *(p+size-i));
      p += size;
    }
    break;
  }
}

#undef SWAP
